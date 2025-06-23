"""
Lightweight orchestrator that composes existing build_xxx scripts.

This follows proper functional design:
- Each build_xxx script is an independent, testable unit
- This orchestrator is just a thin coordination layer
- No duplication of logic - pure composition
- Easy to test individual components in isolation

Usage:
    # Rebuild everything
    python scripts/orchestrate_build.py

    # Rebuild specific database
    python scripts/orchestrate_build.py postgres demo

    # With debug logging  
    python scripts/orchestrate_build.py postgres demo DEBUG
"""

import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    DEFAULT_DATABASE_NAME,
    DEFAULT_DB_ENGINE_TYPE, 
    DEFAULT_LOG_LEVEL,
    DEFAULT_USER_NAME,
    DEFAULT_HOST,
    DEFAULT_PORT,
)
from utils.utils import configure_logging

logger = logging.getLogger(__name__)


class BuildOrchestrator:
    """
    Lightweight orchestrator that composes existing build scripts.
    
    Philosophy: Don't duplicate - coordinate!
    """
    
    def __init__(self, engine_type: str, user_name: str, host: str, port: int, log_level: str):
        self.engine_type = engine_type
        self.user_name = user_name  
        self.host = host
        self.port = port
        self.log_level = log_level
        self.script_dir = Path(__file__).parent
        self.python_exe = self.script_dir.parent / ".venv" / "Scripts" / "python.exe"
    
    def run_script(self, script_name: str, *args) -> Dict[str, Any]:
        """
        Run a build script with given arguments.
        Returns execution result with success/failure info.
        
        Handles both current scripts (no proper exit codes) and enhanced scripts.
        """
        script_path = self.script_dir / script_name
        cmd_args = [str(self.python_exe), str(script_path)] + list(args)
        
        logger.info(f"🔧 Running: {script_name} {' '.join(args)}")
        
        try:
            result = subprocess.run(
                cmd_args,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',  # Replace invalid characters instead of failing
                timeout=300  # 5 minute timeout
            )
            
            # Success based on return code AND smart output analysis
            success = result.returncode == 0
            
            # Enhanced error detection - only flag real errors, not normal logging
            if success and result.stderr:
                # Look for actual error patterns, not just keywords in normal logging
                real_error_patterns = [
                    "Traceback (most recent call last):",  # Python tracebacks
                    "CRITICAL -",   # Critical log level
                    "FATAL -",      # Fatal errors
                    "SyntaxError:", # Python syntax errors
                    "ImportError:", # Import failures
                    "ModuleNotFoundError:", # Missing modules
                    "ConnectionError:", # Database connection issues
                    "psycopg2.Error", # Database errors
                    "sqlalchemy.exc", # SQLAlchemy errors
                    "💥 Fatal error", # Our own fatal error marker
                ]
                
                # Check for actual script failures vs normal info messages
                if any(pattern in result.stderr for pattern in real_error_patterns):
                    success = False
                    logger.warning(f"⚠️  {script_name} returned 0 but stderr contains actual error patterns")
                else:
                    # stderr likely contains normal logging output - trust the exit code
                    logger.debug(f"✅ {script_name} has stderr but appears to be normal logging output")
            
            if success:
                logger.info(f"✅ {script_name} completed successfully")
                # Log key success indicators if present
                if "completed" in result.stdout.lower():
                    logger.debug(f"   {script_name} stdout indicates completion")
            else:
                logger.error(f"❌ {script_name} failed with return code {result.returncode}")
                if result.stderr:
                    logger.error(f"   Error output: {result.stderr[:500]}...")
                if result.stdout and "error" in result.stdout.lower():
                    logger.error(f"   Stdout errors: {result.stdout[:300]}...")
            
            return {
                "script": script_name,
                "success": success,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "output_analysis": "checked for error patterns in output"
            }
            
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ {script_name} timed out after 5 minutes")
            return {
                "script": script_name,
                "success": False,
                "error": "timeout",
                "timeout": True
            }
        except Exception as e:
            logger.error(f"💥 Failed to run {script_name}: {e}")
            return {
                "script": script_name,
                "success": False,
                "error": str(e)
            }
    
    def build_database(self, database_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Orchestrate building a database using existing scripts.
        
        Build order:
        1. build_databases.py - Create database
        2. build_schemas.py - Create schemas  
        3. build_tables.py - Create tables
        4. build_relationships.py - Create FK constraints
        5. build_triggers.py - Set up temporal triggers
        6. build_reference_data_improved.py - Populate reference data
        """
        
        if database_name:
            logger.info(f"🎯 Building specific database: {database_name}")
        else:
            logger.info("🌍 Building all databases")
            
        # Common arguments for all scripts
        common_args = [
            self.engine_type,
            database_name or DEFAULT_DATABASE_NAME,
            self.log_level,
            self.user_name,
            self.host,
            str(self.port)
        ]
        
        build_results = {
            "database": database_name,
            "steps": {},
            "overall_success": True
        }
        
        # Step 1: Create databases
        logger.info("🏗️  Step 1: Creating databases")
        result = self.run_script("build_databases.py", *common_args)
        build_results["steps"]["databases"] = result
        if not result["success"]:
            build_results["overall_success"] = False
            return build_results
        
        # Step 2: Create schemas
        logger.info("📋 Step 2: Creating schemas")
        result = self.run_script("build_schemas.py", *common_args)
        build_results["steps"]["schemas"] = result
        if not result["success"]:
            build_results["overall_success"] = False
            return build_results
        
        # Step 3: Create tables
        logger.info("🗃️  Step 3: Creating tables")
        result = self.run_script("build_tables.py", *common_args)
        build_results["steps"]["tables"] = result
        if not result["success"]:
            build_results["overall_success"] = False
            return build_results
        
        # Step 4: Create relationships (FK constraints)
        logger.info("🔗 Step 4: Creating relationships")
        result = self.run_script("build_relationships.py", *common_args)
        build_results["steps"]["relationships"] = result
        if not result["success"]:
            build_results["overall_success"] = False
            return build_results
        
        # Step 5: Set up triggers
        logger.info("⚡ Step 5: Setting up triggers")
        result = self.run_script("build_triggers.py", *common_args)
        build_results["steps"]["triggers"] = result
        # Note: Don't fail overall build if triggers fail - they may not be needed for all tables
        
        # Step 6: Populate reference data
        logger.info("📊 Step 6: Populating reference data")
        result = self.run_script("build_reference_data_improved.py", *common_args)
        build_results["steps"]["reference_data"] = result
        # Note: Don't fail overall build if reference data fails - it's not critical
        
        return build_results
    
    def build_all_databases(self) -> Dict[str, Any]:
        """
        Build all databases by calling existing discovery logic.
        """
        logger.info("🌍 Discovering and building all databases")
        
        # Use existing discovery from build_databases.py
        # Pass empty string to trigger "all databases" mode
        result = self.run_script(
            "build_databases.py",
            self.engine_type,
            "",  # Empty string triggers "all databases" mode
            self.log_level,
            self.user_name,
            self.host,
            str(self.port)
        )
        
        if not result["success"]:
            return {
                "all_databases": False,
                "error": "Failed to discover/create databases",
                "details": result
            }
        
        # Then build schemas, tables, and reference data for all
        all_results = {
            "all_databases": True,  # Start with success, will be set to False if any step fails
            "databases_created": result,
            "subsequent_steps": {}
        }
        
        # For schemas and tables, the existing scripts have "all databases" modes
        steps = [
            ("schemas", "build_schemas.py"),
            ("tables", "build_tables.py"),
            ("relationships", "build_relationships.py"),
            ("triggers", "build_triggers.py"),
            ("reference_data", "build_reference_data_improved.py")
        ]
        
        for step_name, script_name in steps:
            logger.info(f"🔧 Running {step_name} for all databases")
            result = self.run_script(
                script_name,
                self.engine_type,
                "",  # Empty string triggers "all databases" mode
                self.log_level,
                self.user_name,
                self.host,
                str(self.port)
            )
            all_results["subsequent_steps"][step_name] = result
            
            # Track overall success - if any step fails, mark overall as failed
            if not result["success"]:
                all_results["all_databases"] = False
                logger.error(f"❌ Step {step_name} failed, marking overall build as failed")
        
        return all_results


def print_build_summary(results: Dict[str, Any]):
    """Print summary of build results"""
    logger.info("=" * 60)
    logger.info("🏗️  BUILD ORCHESTRATION SUMMARY")
    logger.info("=" * 60)
    
    if "all_databases" in results:
        # All databases mode
        overall_success = results["all_databases"]
        logger.info(f"🌍 Mode: All Databases")
        logger.info(f"📊 Overall Status: {'✅ SUCCESS' if overall_success else '❌ FAILED'}")
        
        # Show breakdown of steps
        if "databases_created" in results:
            db_result = results["databases_created"]
            status = "✅" if db_result.get("success", False) else "❌"
            logger.info(f"   {status} databases_created")
        
        if "subsequent_steps" in results:
            for step_name, step_result in results["subsequent_steps"].items():
                status = "✅" if step_result.get("success", False) else "❌"
                logger.info(f"   {status} {step_name}")
                
                if not step_result.get("success", False):
                    if "error" in step_result:
                        logger.info(f"      Error: {step_result['error']}")
                    elif step_result.get("stderr"):
                        logger.info(f"      Stderr: {step_result['stderr'][:200]}...")
        
        # Show any top-level error
        if not overall_success and "error" in results:
            logger.info(f"🚨 Error: {results['error']}")
            
        return
    
    # Single database mode
    database = results.get("database", "unknown")
    success = results.get("overall_success", False)
    
    logger.info(f"🎯 Database: {database}")
    logger.info(f"📊 Overall Status: {'✅ SUCCESS' if success else '❌ FAILED'}")
    
    steps = results.get("steps", {})
    for step_name, step_result in steps.items():
        status = "✅" if step_result.get("success", False) else "❌"
        logger.info(f"   {status} {step_name}")
        
        if not step_result.get("success", False) and "error" in step_result:
            logger.info(f"      Error: {step_result['error']}")


if __name__ == "__main__":
    engine_type = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_ENGINE_TYPE
    database_name = sys.argv[2] if len(sys.argv) > 2 else None
    log_level = sys.argv[3].upper() if len(sys.argv) > 3 else DEFAULT_LOG_LEVEL
    user_name = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_USER_NAME
    host = sys.argv[5] if len(sys.argv) > 5 else DEFAULT_HOST
    port = int(sys.argv[6]) if len(sys.argv) > 6 else DEFAULT_PORT

    # Configure logging
    configure_logging(log_level)
    
    # Determine what will actually be built
    target_database = database_name or DEFAULT_DATABASE_NAME
    scope = target_database if target_database else "ALL"
    
    logger.info("🚀 Starting lightweight build orchestration")
    logger.info(f"📋 Engine: {engine_type}, Database: {scope}, Log Level: {log_level}")
    logger.info(f"📋 Connection: user={user_name}, host={host}, port={port}")

    # Create orchestrator
    orchestrator = BuildOrchestrator(
        engine_type=engine_type,
        user_name=user_name,
        host=host, 
        port=port,
        log_level=log_level
    )

    try:
        # Determine actual target: either from command line or config default
        target_database = database_name or DEFAULT_DATABASE_NAME
        
        if target_database:
            # Build specific database (either from command line or config default)
            results = orchestrator.build_database(target_database)
        else:
            # Build all databases (only when both command line and config are empty)
            results = orchestrator.build_all_databases()

        print_build_summary(results)
        
        # Determine exit code
        if "overall_success" in results:
            success = results["overall_success"]
        else:
            success = results.get("all_databases", False) is not False
        
        if success:
            logger.info("🎉 Build orchestration completed successfully!")
            sys.exit(0)
        else:
            logger.error("💥 Build orchestration failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 Fatal error during orchestration: {e}")
        sys.exit(1)
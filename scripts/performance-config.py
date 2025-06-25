#!/usr/bin/env python3
"""
🎯 Automatic Performance Configuration Detection & Management

This script automatically detects machine capacity and recommends optimal
configuration for the medallion lakehouse platform.

Usage:
    python scripts/performance-config.py [--auto] [--config lite|performance|balanced]
    
Examples:
    python scripts/performance-config.py                    # Show recommendations
    python scripts/performance-config.py --auto             # Auto-configure and start
    python scripts/performance-config.py --config lite     # Force lite config
"""

import argparse
import os
import platform
import psutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Tuple


class PerformanceConfig:
    """Detect machine resources and recommend optimal configuration"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.total_memory_gb = round(psutil.virtual_memory().total / (1024**3), 1)
        self.cpu_count = psutil.cpu_count()
        self.available_memory_gb = round(psutil.virtual_memory().available / (1024**3), 1)
        
    def detect_optimal_config(self) -> Tuple[str, Dict[str, any]]:
        """Detect optimal configuration based on machine resources"""
        
        # Realistic performance thresholds
        if self.total_memory_gb > 32 and self.cpu_count >= 12:
            return "performance", {
                "description": "High-Performance Configuration",
                "target": "High-end workstations and desktop development rigs",
                "spark_driver_memory": "2g",
                "spark_executor_memory": "4g",
                "container_memory_scheduler": "8G",
                "expected_performance": "Maximum speed, parallel processing",
                "estimated_dag_time": "20-30 seconds"
            }
        elif self.total_memory_gb > 16 and self.total_memory_gb <= 32 and self.cpu_count >= 8:
            return "balanced", {
                "description": "Balanced Configuration", 
                "target": "Better developer machines and small workstations",
                "spark_driver_memory": "1g",
                "spark_executor_memory": "2g", 
                "container_memory_scheduler": "4G",
                "expected_performance": "Good balance of speed and stability",
                "estimated_dag_time": "30-45 seconds"
            }
        else:
            return "lite", {
                "description": "Lite Configuration",
                "target": "Most developer laptops, CI environments (≤16GB RAM)",
                "spark_driver_memory": "512m",
                "spark_executor_memory": "512m",
                "container_memory_scheduler": "2G", 
                "expected_performance": "Stable execution, conservative resource usage",
                "estimated_dag_time": "45-60 seconds"
            }
    
    def show_system_info(self):
        """Display current system information"""
        print("🖥️  System Information:")
        print(f"   OS: {platform.system()} {platform.release()}")
        print(f"   Total Memory: {self.total_memory_gb} GB")
        print(f"   Available Memory: {self.available_memory_gb} GB")
        print(f"   CPU Cores: {self.cpu_count}")
        print(f"   Architecture: {platform.machine()}")
        print()
    
    def show_recommendations(self):
        """Show configuration recommendations for all tiers"""
        print("🎯 Configuration Recommendations:")
        print()
        
        configs = ["lite", "balanced", "performance"]
        current_config, current_details = self.detect_optimal_config()
        
        for config in configs:
            if config == "lite":
                details = {
                    "description": "Lite Configuration",
                    "target": "≤16GB RAM, most developer laptops, CI/testing",
                    "spark_driver_memory": "512m",
                    "spark_executor_memory": "512m",
                    "estimated_dag_time": "45-60 seconds"
                }
            elif config == "balanced":
                details = {
                    "description": "Balanced Configuration", 
                    "target": "16-32GB RAM, better developer machines",
                    "spark_driver_memory": "1g",
                    "spark_executor_memory": "2g",
                    "estimated_dag_time": "30-45 seconds"
                }
            else:  # performance
                details = {
                    "description": "High-Performance Configuration",
                    "target": ">32GB RAM, high-end workstations",
                    "spark_driver_memory": "2g", 
                    "spark_executor_memory": "4g",
                    "estimated_dag_time": "20-30 seconds"
                }
            
            recommended = "← RECOMMENDED" if config == current_config else ""
            print(f"   {config.upper():12} {details['description']} {recommended}")
            print(f"               Target: {details['target']}")
            print(f"               Spark: {details['spark_driver_memory']} driver, {details['spark_executor_memory']} executor")
            print(f"               DAG Time: {details['estimated_dag_time']}")
            print()
    
    def apply_configuration(self, config_name: str):
        """Apply the specified configuration"""
        print(f"🚀 Applying {config_name} configuration...")
        
        # Stop existing containers
        print("   Stopping existing containers...")
        subprocess.run([
            "docker-compose", "down"
        ], cwd=self.project_root, capture_output=True)
        
        # Build compose command based on configuration
        if config_name == "performance":
            compose_files = ["-f", "docker-compose.yml", "-f", "docker-compose.performance.yml"]
        elif config_name == "lite":
            compose_files = ["-f", "docker-compose.yml", "-f", "docker-compose.lite.yml"]
        else:  # balanced (default docker-compose.yml)
            compose_files = ["-f", "docker-compose.yml"]
        
        # Start with selected configuration
        print(f"   Starting containers with {config_name} configuration...")
        cmd = ["docker-compose"] + compose_files + ["up", "-d"]
        
        result = subprocess.run(cmd, cwd=self.project_root, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Successfully started with {config_name} configuration!")
            print(f"   Airflow UI: http://localhost:8080")
            print(f"   Default credentials: admin/admin")
            self._save_current_config(config_name)
        else:
            print(f"❌ Failed to start containers:")
            print(result.stderr)
            sys.exit(1)
    
    def _save_current_config(self, config_name: str):
        """Save current configuration to file"""
        config_file = self.project_root / ".current-config"
        with open(config_file, "w") as f:
            f.write(config_name)
    
    def get_current_config(self) -> str:
        """Get currently applied configuration"""
        config_file = self.project_root / ".current-config"
        if config_file.exists():
            return config_file.read_text().strip()
        return "unknown"


def main():
    parser = argparse.ArgumentParser(
        description="Automatic performance configuration for medallion lakehouse platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/performance-config.py                    # Show recommendations
  python scripts/performance-config.py --auto             # Auto-configure and start  
  python scripts/performance-config.py --config lite     # Force lite config
  python scripts/performance-config.py --config performance  # Force high-performance
        """
    )
    
    parser.add_argument(
        "--auto", 
        action="store_true",
        help="Automatically apply recommended configuration and start services"
    )
    
    parser.add_argument(
        "--config",
        choices=["lite", "balanced", "performance"],
        help="Force specific configuration tier"
    )
    
    parser.add_argument(
        "--quiet", "-q",
        action="store_true", 
        help="Minimal output"
    )
    
    args = parser.parse_args()
    
    config = PerformanceConfig()
    
    if not args.quiet:
        print("🎯 Medallion Lakehouse Performance Configuration")
        print("=" * 50)
        print()
        config.show_system_info()
    
    # Get recommended configuration
    recommended_config, recommended_details = config.detect_optimal_config()
    
    if args.config:
        # Force specific configuration
        target_config = args.config
        if not args.quiet:
            print(f"🔧 Forcing {target_config} configuration (overriding recommendation: {recommended_config})")
            print()
    elif args.auto:
        # Use recommended configuration
        target_config = recommended_config
        if not args.quiet:
            print(f"🤖 Auto-applying recommended configuration: {target_config}")
            print()
    else:
        # Show recommendations only
        config.show_recommendations()
        current = config.get_current_config()
        if current != "unknown":
            print(f"Current Configuration: {current.upper()}")
        print()
        print("To apply a configuration:")
        print(f"   python scripts/performance-config.py --auto                    # Apply recommended ({recommended_config})")
        print( "   python scripts/performance-config.py --config lite           # Apply lite")
        print( "   python scripts/performance-config.py --config performance    # Apply high-performance")
        return
    
    # Apply configuration
    config.apply_configuration(target_config)
    
    if not args.quiet:
        print()
        print("🎉 Configuration applied successfully!")
        print()
        print("Next steps:")
        print("   1. Wait 30-60 seconds for services to start")
        print("   2. Open http://localhost:8080 (admin/admin)")
        print("   3. Trigger bronze_basic_pagila_ingestion DAG to test")
        print()
        print("To switch configurations later:")
        print("   python scripts/performance-config.py --config [lite|balanced|performance]")


if __name__ == "__main__":
    main()
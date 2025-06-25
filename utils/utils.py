import importlib
import importlib.util
import logging
import os
import sys
from types import ModuleType
from typing import List, Type

from sqlalchemy import DDL, event
from sqlalchemy.engine import Engine
from sqlmodel import SQLModel

from engines.postgres_engine import get_engine as get_postgres_engine
from engines.sqlite_engine import get_engine as get_sqlite_engine


def configure_logging(log_level: str):
    if hasattr(configure_logging, "has_run"):
        return

    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    # Remove all handlers associated with the root logger object
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Configure the root logger with UTF-8 encoding support
    import sys
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    
    # Configure encoding for Windows compatibility
    if hasattr(handler.stream, 'reconfigure'):
        try:
            handler.stream.reconfigure(encoding='utf-8', errors='replace')
        except:
            pass  # Fallback if reconfigure fails
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,  # This overwrites any existing logging configuration
        handlers=[handler]
    )

    # Configure SQLAlchemy logging
    sqlalchemy_logger = logging.getLogger("sqlalchemy.engine")
    sqlalchemy_logger.handlers = []  # Remove any existing handlers
    sqlalchemy_logger.propagate = False  # Prevent propagation to root logger
    #
    # if log_level.upper() == 'DEBUG':
    #     sqlalchemy_logger.setLevel(logging.INFO)
    # elif log_level.upper() == 'INFO':
    #     sqlalchemy_logger.setLevel(logging.WARNING)
    # else:
    #     sqlalchemy_logger.setLevel(logging.ERROR)
    #
    # # Add a single handler to sqlalchemy logger
    # handler = logging.StreamHandler()
    # handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    # sqlalchemy_logger.addHandler(handler)

    # Log the configuration
    logger = logging.getLogger(__name__)
    logger.info(
        f"Logging configured. Root level: {log_level}"
    )  # , SQLAlchemy level: {sqlalchemy_logger.level}")


def get_engine_from_param(
    db_engine_type: str, database_name: str = "postgres", user_name: str = None, host: str = "localhost", port: int = 5432
) -> Engine:
    logger = logging.getLogger(__name__)

    if db_engine_type == "postgres":
        logger.info(f"Creating PostgreSQL engine for database: {database_name}")
        return get_postgres_engine(echo_msgs=True, database_name=database_name, user_name=user_name, host=host, port=port)
    elif db_engine_type == "sqlite":
        logger.info("Creating SQLite engine")
        return get_sqlite_engine(echo_msgs=True)
    else:
        logger.error(f"Unsupported database engine type: {db_engine_type}")
        raise ValueError(f"Unsupported database engine type: {db_engine_type}")


# Used to annotate the order in which a schema should be processed
def set_processing_order(order: int):
    def decorator(cls):
        setattr(cls, "_processing_order", order)
        return cls

    return decorator


def discover_schemas(path_to_schemas: str) -> List[Type[SQLModel]]:
    logger = logging.getLogger(__name__)
    logger.debug(f"Discovering schemas in {path_to_schemas}")

    schemas = []
    for filename in os.listdir(path_to_schemas):
        if filename.endswith(".py") and filename != "__init__.py":
            module_name = f"schemas.{filename[:-3]}"
            print(f"Found schema file '{module_name}', preparing for table creation")
            module = importlib.import_module(module_name)
            for name, obj in module.__dict__.items():
                if (
                    isinstance(obj, type)
                    and issubclass(obj, SQLModel)
                    and obj is not SQLModel  # Exclude SQLModel itself
                    and hasattr(obj, "metadata")
                    and getattr(obj, "__abstract__", False)
                ):  # Only include abstract base classes
                    schemas.append(obj)
    return schemas


def is_table_class(obj: Type[SQLModel]) -> bool:
    return (
        isinstance(obj, type)
        and issubclass(obj, SQLModel)
        and getattr(obj, "__tablename__", None) is not None
        and not getattr(obj, "__abstract__", False)
    )


def discover_tables(path_to_tables: str) -> List[ModuleType]:
    logger = logging.getLogger(__name__)
    logger.debug(f"Discovering tables in {path_to_tables}")

    tables = []
    for root, _, files in os.walk(path_to_tables):
        for filename in files:
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = os.path.relpath(
                    os.path.join(root, filename), path_to_tables
                ).replace(os.sep, ".")[:-3]
                logger.debug(f"Found table file '{module_name}', preparing to import")
                module = importlib.import_module(f"tables.{module_name}")
                logger.info(f"Importing module: tables.{module_name}")
                tables.append(module)
    return tables


# Enhanced discovery functions for multi-database architecture
def discover_functions_in_database(src_path: str, database_name: str):
    """Discover and attach SQL functions for a specific database"""
    logger = logging.getLogger(__name__)
    functions_path = os.path.join(src_path, database_name, "functions")
    
    if not os.path.exists(functions_path):
        logger.debug(f"Functions directory not found: {functions_path}")
        return

    # Add parent directory to Python's path if needed
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    for root, _, files in os.walk(functions_path):
        for filename in files:
            if filename.endswith(".py") and filename != "__init__.py":
                # Get the relative path from the functions directory
                rel_path = os.path.relpath(root, functions_path)
                # Construct the module name using package structure
                if rel_path == ".":
                    module_name = f"src.{database_name}.functions.{filename[:-3]}"
                else:
                    module_name = f"src.{database_name}.functions.{rel_path.replace(os.sep, '.')}.{filename[:-3]}"

                file_path = os.path.join(root, filename)
                logger.debug(f"Attempting to import module: {module_name}")

                try:
                    spec = importlib.util.spec_from_file_location(module_name, file_path)
                    if spec is None or spec.loader is None:
                        logger.warning(f"Could not load spec for {file_path}")
                        continue

                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)

                    # First, check for direct DDL objects
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if isinstance(attr, DDL):
                            event.listen(SQLModel.metadata, "before_create", attr)
                            logger.info(f"Attached DDL {attr_name} from {database_name}.{filename}")

                    # Then check for attachment functions (for extensibility)
                    attachment_functions = [attr for attr in dir(module) if attr.startswith('attach_') and attr.endswith('_ddl')]
                    for func_name in attachment_functions:
                        func = getattr(module, func_name)
                        if callable(func):
                            func()
                            logger.info(f"Called attachment function {func_name} from {database_name}.{filename}")

                except Exception as e:
                    logger.error(f"Error processing {module_name}: {str(e)}")
                    logger.error(f"Search path was: {sys.path}")
                    logger.warning(f"Could not import function module {module_name}. "
                                 f"Make sure the package structure is correct and "
                                 f"the parent directory '{src_path}' contains a valid Python package.")
                    # Continue processing other files instead of raising


def discover_tables_in_database(src_path: str, database_name: str):
    """Discover tables in a specific database"""
    logger = logging.getLogger(__name__)
    tables_path = os.path.join(src_path, database_name, "tables")
    
    if not os.path.exists(tables_path):
        logger.warning(f"Tables directory not found: {tables_path}")
        return []

    # Add parent directory to Python's path if needed (improves import reliability)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    tables = []
    for root, _, files in os.walk(tables_path):
        for filename in files:
            if filename.endswith(".py") and filename != "__init__.py":
                # Calculate relative path from tables directory
                rel_path = os.path.relpath(root, tables_path)
                # Construct the module name using package structure
                if rel_path == ".":
                    module_name = f"src.{database_name}.tables.{filename[:-3]}"
                else:
                    module_name = f"src.{database_name}.tables.{rel_path.replace(os.sep, '.')}.{filename[:-3]}"
                
                logger.debug(f"Attempting to import module: {module_name}")
                try:
                    module = importlib.import_module(module_name)
                    logger.info(f"Importing module: {module_name}")
                    tables.append(module)
                except ImportError as e:
                    logger.error(f"Failed to import {module_name}: {e}")
                    logger.error(f"Search path was: {sys.path}")
                    logger.warning(f"Could not import table module {module_name}. "
                                 f"Make sure the package structure is correct and "
                                 f"the parent directory '{src_path}' contains a valid Python package.")
                    # Continue processing other files instead of raising
    return tables


def discover_all_databases_with_tables(src_path):
    """Discover all database directories in src/ that have tables"""
    logger = logging.getLogger(__name__)
    databases = []
    for item in os.listdir(src_path):
        item_path = os.path.join(src_path, item)
        if os.path.isdir(item_path) and not item.startswith('_'):
            tables_path = os.path.join(item_path, "tables")
            if os.path.exists(tables_path):
                databases.append(item)
                logger.debug(f"Found database with tables: {item}")
    return databases


def discover_schemas_in_database(src_path, database_name):
    """Discover schemas in a specific database with enhanced error handling"""
    logger = logging.getLogger(__name__)
    schemas_path = os.path.join(src_path, database_name, "schemas")
    logger.debug(f"Discovering schemas in {schemas_path}")
    
    if not os.path.exists(schemas_path):
        logger.warning(f"Schemas directory not found: {schemas_path}")
        return []

    # Add parent directory to Python's path if needed (improves import reliability)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    schemas = []
    for filename in os.listdir(schemas_path):
        if filename.endswith(".py") and filename != "__init__.py":
            # Construct module name using package structure  
            module_name = f"src.{database_name}.schemas.{filename[:-3]}"
            logger.debug(f"Attempting to import module: {module_name}")
            
            try:
                module = importlib.import_module(module_name)
                for name, obj in module.__dict__.items():
                    if (
                        isinstance(obj, type)
                        and issubclass(obj, SQLModel)
                        and name.endswith("Schema")
                    ):
                        if hasattr(obj, "metadata") and hasattr(obj.metadata, "schema"):
                            schema_name = obj.metadata.schema
                            processing_order = getattr(
                                obj, "_processing_order", 100
                            )  # Default to 100 if not set
                            logger.debug(
                                f"Found schema: {schema_name} with processing order {processing_order}"
                            )
                            schemas.append((obj, processing_order))
            except ImportError as e:
                logger.error(f"Failed to import {module_name}: {e}")
                logger.error(f"Search path was: {sys.path}")
                logger.warning(f"Could not import schema module {module_name}. "
                             f"Make sure the package structure is correct and "
                             f"the parent directory '{src_path}' contains a valid Python package.")
                # Continue processing other files instead of raising

    if not schemas:
        logger.warning(f"No schema definitions found in {schemas_path}")

    # Sort schemas by their processing order
    schemas.sort(key=lambda x: x[1])
    return schemas


def discover_all_databases_with_schemas(src_path):
    """Discover all database directories in src/ that have schemas"""
    logger = logging.getLogger(__name__)
    databases = []
    for item in os.listdir(src_path):
        item_path = os.path.join(src_path, item)
        if os.path.isdir(item_path) and not item.startswith('_'):
            schemas_path = os.path.join(item_path, "schemas")
            if os.path.exists(schemas_path):
                databases.append(item)
                logger.debug(f"Found database with schemas: {item}")
    return databases


def discover_all_databases(src_path):
    """Discover all database directories in src/ that have database scripts"""
    logger = logging.getLogger(__name__)
    logger.debug(f"Discovering databases in {src_path}")

    databases = []
    for item in os.listdir(src_path):
        item_path = os.path.join(src_path, item)
        if os.path.isdir(item_path) and not item.startswith('_'):
            db_script_path = os.path.join(item_path, f"{item}.py")
            if os.path.exists(db_script_path):
                databases.append(item)
                logger.debug(f"Found database with script: {item}")
    return databases


def discover_databases_with_functions(src_path):
    """Discover databases with create_target_db functions with enhanced error handling"""
    logger = logging.getLogger(__name__)
    
    # Add parent directory to Python's path if needed (improves import reliability)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    # First discover all databases, then check for functions
    database_names = discover_all_databases(src_path)
    databases_with_functions = []
    
    for item in database_names:
        # Construct module name using package structure
        module_name = f"src.{item}.{item}"
        logger.debug(f"Attempting to import module: {module_name}")
        
        try:
            module = importlib.import_module(module_name)
            for name, obj in module.__dict__.items():
                if callable(obj) and name == "create_target_db":
                    logger.debug(f"Found create_target_db function in {module_name}")
                    databases_with_functions.append((obj, item))  # Include database name for better tracking
                    break  # Found the function, move to next database
        except ImportError as e:
            logger.error(f"Failed to import {module_name}: {e}")
            logger.error(f"Search path was: {sys.path}")
            logger.warning(f"Could not import database module {module_name}. "
                         f"Make sure the package structure is correct and "
                         f"the parent directory '{src_path}' contains a valid Python package.")
            # Continue processing other databases instead of raising

    if not databases_with_functions:
        logger.warning(f"No database creation functions found in {src_path}")

    return databases_with_functions


def create_database_function_for_name(src_path, database_name):
    """Get the create_target_db function for a specific database"""
    logger = logging.getLogger(__name__)
    
    # Add parent directory to Python's path if needed (improves import reliability)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    module_name = f"src.{database_name}.{database_name}"
    logger.debug(f"Attempting to import module: {module_name}")
    
    try:
        module = importlib.import_module(module_name)
        for name, obj in module.__dict__.items():
            if callable(obj) and name == "create_target_db":
                logger.debug(f"Found create_target_db function in {module_name}")
                return obj
        logger.error(f"No create_target_db function found in {module_name}")
        return None
    except ImportError as e:
        logger.error(f"Failed to import {module_name}: {e}")
        logger.error(f"Search path was: {sys.path}")
        logger.error(f"Could not import database module {module_name}. "
                     f"Make sure the package structure is correct and "
                     f"the parent directory '{src_path}' contains a valid Python package.")
        return None


def extract_table_classes_from_modules(modules):
    """Extract SQLModel table classes from imported modules"""
    logger = logging.getLogger(__name__)
    tables = []
    total_classes_examined = 0
    
    logger.info(f"Examining {len(modules)} modules for table classes...")
    
    for i, module in enumerate(modules):
        module_name = getattr(module, '__name__', f'unknown_module_{i}')
        logger.info(f"🔍 Module {i+1}/{len(modules)}: {module_name}")
        
        module_classes = []
        all_attrs = list(module.__dict__.items())
        logger.info(f"  📦 Module has {len(all_attrs)} total attributes")
        
        type_count = 0
        for name, obj in all_attrs:
            if isinstance(obj, type):
                type_count += 1
                total_classes_examined += 1
                
                # Deep inspection
                has_tablename = hasattr(obj, '__tablename__')
                # Check for explicit __abstract__ = True in the class itself, not inherited
                is_abstract = obj.__dict__.get('__abstract__', False)
                has_table_param = getattr(obj, '__table__', None) is not None
                is_sqlmodel = hasattr(obj, '__table__') or str(type(obj).__bases__).find('SQLModel') >= 0
                
                logger.info(f"    🔸 Class {name}: tablename={has_tablename}, abstract={is_abstract}, has_table={has_table_param}, sqlmodel_related={is_sqlmodel}")
                
                if has_tablename:
                    tablename_value = getattr(obj, '__tablename__', 'NO_VALUE')
                    logger.info(f"      📋 __tablename__ = '{tablename_value}'")
                
                # Better detection: has tablename AND either has __table__ OR explicit table=True 
                if has_tablename and (has_table_param or not is_abstract):
                    tables.append(obj)
                    module_classes.append(f"{name} -> {obj.__tablename__}")
                    logger.info(f"      ✅ MATCH! Added table class: {name} -> {obj.__tablename__}")
        
        logger.info(f"  📊 Module summary: {type_count} classes, {len(module_classes)} tables found")
        if module_classes:
            logger.info(f"  🎯 Tables: {', '.join(module_classes)}")
    
    logger.info(f"🏁 FINAL: {len(tables)} tables found from {total_classes_examined} classes examined across {len(modules)} modules")
    return tables


def discover_database_tables(src_path: str, database_name: str, return_type='classes'):
    """
    DRY unified table discovery - eliminates duplication between discover_tables_in_database + extract_table_classes_from_modules
    
    Args:
        src_path: Path to src directory
        database_name: Name of database to discover
        return_type: 'modules' or 'classes' - what to return
    
    Returns:
        List of modules OR list of table classes, depending on return_type
    """
    logger = logging.getLogger(__name__)
    
    # Discover and import modules (reuse existing logic)
    table_modules = discover_tables_in_database(src_path, database_name)
    
    if return_type == 'modules':
        return table_modules
    elif return_type == 'classes':
        return extract_table_classes_from_modules(table_modules)
    else:
        raise ValueError(f"Invalid return_type: {return_type}. Must be 'modules' or 'classes'")

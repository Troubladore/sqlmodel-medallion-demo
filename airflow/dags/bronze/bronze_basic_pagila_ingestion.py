"""
🥉 BRONZE BASIC - Pagila Ingestion DAG

Purpose: Orchestrate the ingestion of Pagila source data into bronze basic layer
Schedule: Daily full refresh of source tables
Architecture: Python native Airflow DAG with PySpark tasks

DAG Structure:
- Extract source Pagila tables (actor, customer, film, language, payment, rental)
- Transform and load into bronze layer with audit fields
- Validate data quality and record counts
- Send notifications on success/failure

Dependencies:
- Source Pagila database must be accessible
- Bronze basic database and tables must exist
- Spark cluster/session must be available
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# Import our transform job
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from src.bronze_basic.transforms.pagila_to_bronze import run as bronze_transform


# Default arguments for all tasks
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG configuration
DAG_ID = 'bronze_basic_pagila_ingestion'
SCHEDULE_INTERVAL = '@daily'  # Run daily
DESCRIPTION = 'Ingest Pagila source data into Bronze Basic layer'


def check_source_connectivity(**context) -> Dict[str, Any]:
    """Check connectivity to source Pagila database"""
    from sqlalchemy import create_engine, text
    
    try:
        # Test connection to source Pagila database
        source_url = "postgresql://postgres:postgres@host.docker.internal:5432/pagila"
        source_engine = create_engine(source_url)
        
        with source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as connection_test"))
            row = result.fetchone()
            
        return {
            "status": "success",
            "message": "Source database connection successful",
            "test_result": row[0] if row else None
        }
        
    except Exception as e:
        raise Exception(f"Source database connection failed: {str(e)}")


def check_target_connectivity(**context) -> Dict[str, Any]:
    """Check connectivity to target Bronze Basic database"""
    from sqlalchemy import create_engine, text
    
    try:
        # Test connection to bronze_basic database
        target_url = "postgresql://postgres:postgres@host.docker.internal:5432/bronze_basic"
        target_engine = create_engine(target_url)
        
        with target_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as connection_test"))
            row = result.fetchone()
            
        return {
            "status": "success", 
            "message": "Target database connection successful",
            "test_result": row[0] if row else None
        }
        
    except Exception as e:
        raise Exception(f"Target database connection failed: {str(e)}")


def run_bronze_ingestion(**context) -> Dict[str, Any]:
    """Execute the main bronze ingestion transform"""
    from pyspark.sql import SparkSession
    
    # Get execution date from Airflow context
    execution_date = context['ds']  # YYYY-MM-DD format
    
    # Initialize Spark session with Spark 4.0.0 optimizations
    spark = (SparkSession.builder
             .appName(f"Bronze_Basic_Pagila_{execution_date}")
             # Enhanced Adaptive Query Execution (AQE) in Spark 4.0.0
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
             .config("spark.sql.adaptive.skewJoin.enabled", "true")
             # ANSI SQL mode for better data integrity (default in 4.0.0)
             .config("spark.sql.ansi.enabled", "true")
             # Enhanced performance settings
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             # JDBC driver configuration
             .config("spark.jars", "/opt/spark/jars/postgresql.jar")
             .getOrCreate())
    
    try:
        # Create configuration dictionary
        db_config = {
            'host': 'host.docker.internal',  # External PostgreSQL host
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres'
        }
        
        # Run bronze ingestion transform
        results = bronze_transform(spark, db_config, execution_date)
        
        # Push results to XCom for downstream tasks
        context['task_instance'].xcom_push(key='ingestion_results', value=results)
        
        return results
        
    finally:
        spark.stop()


def validate_bronze_data(**context) -> Dict[str, Any]:
    """Validate the ingested bronze data"""
    from sqlalchemy import create_engine, text
    
    # Get ingestion results from upstream task
    ti = context['task_instance']
    ingestion_results = ti.xcom_pull(task_ids='run_bronze_transform', key='ingestion_results')
    
    target_url = "postgresql://postgres:postgres@host.docker.internal:5432/bronze_basic"
    target_engine = create_engine(target_url)
    
    validation_results = {
        "execution_date": context['ds'],
        "batch_id": ingestion_results.get('batch_id'),
        "table_validations": [],
        "total_records_validated": 0,
        "validation_errors": []
    }
    
    # Define expected tables and basic validation rules
    bronze_tables = [
        'staging_pagila.br_actor',
        'staging_pagila.br_customer', 
        'staging_pagila.br_film',
        'staging_pagila.br_language',
        'staging_pagila.br_payment',
        'staging_pagila.br_rental'
    ]
    
    try:
        with target_engine.connect() as conn:
            for table in bronze_tables:
                # Basic record count validation
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                record_count = count_result.fetchone()[0]
                
                # Check for required audit fields
                audit_check = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM {table} 
                    WHERE br_load_time IS NOT NULL 
                    AND br_batch_id = '{ingestion_results.get('batch_id')}'
                """))
                audit_count = audit_check.fetchone()[0]
                
                table_validation = {
                    "table": table,
                    "record_count": record_count,
                    "audit_records": audit_count,
                    "validation_passed": record_count > 0 and audit_count == record_count
                }
                
                validation_results["table_validations"].append(table_validation)
                validation_results["total_records_validated"] += record_count
                
                if not table_validation["validation_passed"]:
                    validation_results["validation_errors"].append(
                        f"Table {table} failed validation: records={record_count}, audit_records={audit_count}"
                    )
        
        # Overall validation status
        validation_results["overall_success"] = len(validation_results["validation_errors"]) == 0
        
        return validation_results
        
    except Exception as e:
        validation_results["validation_errors"].append(f"Validation failed: {str(e)}")
        validation_results["overall_success"] = False
        return validation_results


def send_completion_notification(**context) -> None:
    """Send notification about DAG completion status"""
    # Get results from previous tasks
    ti = context['task_instance']
    ingestion_results = ti.xcom_pull(task_ids='run_bronze_transform', key='ingestion_results')
    validation_results = ti.xcom_pull(task_ids='validate_bronze_data')
    
    # Create summary message
    summary = {
        "dag_id": DAG_ID,
        "execution_date": context['ds'],
        "batch_id": ingestion_results.get('batch_id'),
        "tables_processed": len(ingestion_results.get('tables_processed', [])),
        "total_rows": ingestion_results.get('total_rows_processed', 0),
        "validation_passed": validation_results.get('overall_success', False),
        "processing_errors": len(ingestion_results.get('errors', [])),
        "validation_errors": len(validation_results.get('validation_errors', []))
    }
    
    print(f"BRONZE BASIC INGESTION SUMMARY: {summary}")
    
    # In a real implementation, you would send this to Slack, email, or monitoring system
    return summary


# Create the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    catchup=False,
    tags=['bronze', 'basic', 'pagila', 'ingestion']
) as dag:
    
    # Start task
    start_task = DummyOperator(
        task_id='start_bronze_ingestion',
        doc_md="""
        ## Bronze Basic Pagila Ingestion Start
        
        Starting the daily ingestion of Pagila source data into the Bronze Basic layer.
        This DAG will:
        1. Check source and target database connectivity
        2. Extract data from Pagila source tables
        3. Transform and load data with bronze audit fields
        4. Validate the ingested data
        5. Send completion notifications
        """
    )
    
    # Pre-flight checks
    with TaskGroup('preflight_checks') as preflight_group:
        
        check_source = PythonOperator(
            task_id='check_source_database',
            python_callable=check_source_connectivity,
            doc_md="Verify connectivity to source Pagila database"
        )
        
        check_target = PythonOperator(
            task_id='check_target_database', 
            python_callable=check_target_connectivity,
            doc_md="Verify connectivity to target Bronze Basic database"
        )
    
    # Main ingestion transform
    run_transform = PythonOperator(
        task_id='run_bronze_transform',
        python_callable=run_bronze_ingestion,
        doc_md="""
        ## Bronze Transform Execution
        
        Executes the main PySpark transform job to:
        - Extract all Pagila source tables
        - Apply lenient typing for data quality resilience
        - Add comprehensive bronze audit fields
        - Load data using full refresh strategy
        """
    )
    
    # Data validation
    validate_data = PythonOperator(
        task_id='validate_bronze_data',
        python_callable=validate_bronze_data,
        doc_md="Validate ingested bronze data for completeness and quality"
    )
    
    # Completion notification
    notify_completion = PythonOperator(
        task_id='send_completion_notification',
        python_callable=send_completion_notification,
        doc_md="Send summary notification about ingestion results"
    )
    
    # End task
    end_task = DummyOperator(
        task_id='end_bronze_ingestion',
        doc_md="Bronze Basic Pagila ingestion completed successfully"
    )
    
    # Define task dependencies
    start_task >> preflight_group >> run_transform >> validate_data >> notify_completion >> end_task
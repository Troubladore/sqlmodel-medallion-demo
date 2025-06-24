"""
🥉 BRONZE CDC - Pagila Change Data Capture Ingestion DAG

Purpose: Orchestrate CDC ingestion from Pagila source into bronze CDC layer
Schedule: Micro-batch processing every 5 minutes for near real-time analytics
Architecture: Python native Airflow DAG with PySpark CDC tasks

DAG Structure:
- Extract CDC changes from source Pagila tables (customer, payment, rental)
- Transform to CDC format with LSN ordering and JSONB payloads
- Load incrementally with watermark-based processing
- Validate CDC data quality and LSN sequencing
- Send real-time notifications on CDC lag or failures

CDC Differences from Bronze Basic:
- Incremental append vs full refresh
- LSN-based ordering vs batch-based processing
- JSONB payloads vs structured columns
- Watermark management vs snapshot processing
- Real-time alerting vs daily summaries

Dependencies:
- Source Pagila database must be accessible
- Bronze CDC database and tables must exist  
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

# Import our CDC transform job
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from src.bronze_cdc.transforms.pagila_cdc_to_bronze import run as cdc_transform


# Default arguments for all tasks
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # More retries for CDC reliability
    'retry_delay': timedelta(minutes=2),  # Shorter delay for CDC
    'catchup': False
}

# DAG configuration - More frequent for CDC
DAG_ID = 'bronze_cdc_ingestion'
SCHEDULE_INTERVAL = '*/5 * * * *'  # Every 5 minutes for near real-time
DESCRIPTION = 'Ingest Pagila CDC changes into Bronze CDC layer'


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
    """Check connectivity to target Bronze CDC database"""
    from sqlalchemy import create_engine, text
    
    try:
        # Test connection to bronze_cdc database
        target_url = "postgresql://postgres:postgres@host.docker.internal:5432/bronze_cdc"
        target_engine = create_engine(target_url)
        
        with target_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as connection_test"))
            row = result.fetchone()
            
        return {
            "status": "success", 
            "message": "Target CDC database connection successful",
            "test_result": row[0] if row else None
        }
        
    except Exception as e:
        raise Exception(f"Target CDC database connection failed: {str(e)}")


def check_cdc_lag(**context) -> Dict[str, Any]:
    """Monitor CDC lag to ensure real-time processing"""
    from sqlalchemy import create_engine, text
    
    try:
        target_url = "postgresql://postgres:postgres@host.docker.internal:5432/bronze_cdc"
        target_engine = create_engine(target_url)
        
        lag_results = {}
        
        with target_engine.connect() as conn:
            # Check lag for each CDC table
            cdc_tables = ['br_customer_cdc', 'br_payment_cdc', 'br_rental_cdc']
            
            for table in cdc_tables:
                try:
                    # Check latest load timestamp - handle empty tables gracefully
                    lag_query = text(f"""
                        SELECT 
                            CASE 
                                WHEN MAX(load_ts) IS NULL THEN 0
                                ELSE EXTRACT(EPOCH FROM (now() - MAX(load_ts)))/60 
                            END as lag_minutes,
                            COUNT(*) as total_records,
                            COALESCE(MAX(lsn), 0) as latest_lsn
                        FROM cdc_bronze.{table}
                    """)
                    
                    result = conn.execute(lag_query)
                    row = result.fetchone()
                    
                    lag_results[table] = {
                        "lag_minutes": float(row[0]) if row[0] is not None else 0,
                        "total_records": int(row[1]) if row[1] is not None else 0,
                        "latest_lsn": int(row[2]) if row[2] is not None else 0
                    }
                    
                except Exception as table_error:
                    lag_results[table] = {"error": str(table_error)}
        
        # Alert if any table has > 10 minute lag
        high_lag_tables = [
            table for table, metrics in lag_results.items() 
            if isinstance(metrics, dict) and metrics.get("lag_minutes", 0) > 10
        ]
        
        return {
            "status": "success",
            "lag_results": lag_results,
            "high_lag_tables": high_lag_tables,
            "alert_required": len(high_lag_tables) > 0
        }
        
    except Exception as e:
        raise Exception(f"CDC lag check failed: {str(e)}")


def run_cdc_ingestion(**context) -> Dict[str, Any]:
    """Execute the main CDC ingestion transform"""
    from pyspark.sql import SparkSession
    
    # Get execution date from Airflow context
    execution_date = context['ds']  # YYYY-MM-DD format
    
    # Initialize Spark session with memory-optimized settings for container environment
    spark = (SparkSession.builder
             .appName(f"Bronze_CDC_Pagila_{execution_date}")
             # Memory settings for container environment
             .config("spark.executor.memory", "1g")
             .config("spark.driver.memory", "1g")
             .config("spark.executor.cores", "2")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
             # Simplified configuration for stability
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Disable Arrow for stability
             # JDBC driver configuration
             .config("spark.jars", "/opt/spark/jars/postgresql.jar")
             # Local mode settings
             .master("local[2]")
             .getOrCreate())
    
    try:
        # Create configuration dictionary (same as bronze_basic)
        db_config = {
            'host': 'host.docker.internal',  # External PostgreSQL host
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres'
        }
        
        # Run CDC ingestion transform
        results = cdc_transform(spark, db_config, execution_date)
        
        # Push results to XCom for downstream tasks
        context['task_instance'].xcom_push(key='cdc_ingestion_results', value=results)
        
        return results
        
    finally:
        spark.stop()


def validate_cdc_data(**context) -> Dict[str, Any]:
    """Validate the ingested CDC data with CDC-specific checks"""
    from sqlalchemy import create_engine, text
    
    # Get ingestion results from upstream task - handle missing XCom gracefully
    ti = context['task_instance']
    ingestion_results = ti.xcom_pull(task_ids='run_cdc_transform', key='cdc_ingestion_results')
    
    # Handle case where XCom data is missing (e.g., individual task testing)
    if ingestion_results is None:
        ingestion_results = {
            'batch_id': f"validation_test_{context['ds']}",
            'cdc_tables_processed': [],
            'total_cdc_rows_processed': 0,
            'errors': []
        }
    
    target_url = "postgresql://postgres:postgres@host.docker.internal:5432/bronze_cdc"
    target_engine = create_engine(target_url)
    
    validation_results = {
        "execution_date": context['ds'],
        "batch_id": ingestion_results.get('batch_id'),
        "cdc_table_validations": [],
        "total_cdc_records_validated": 0,
        "validation_errors": []
    }
    
    # Define CDC tables and validation rules
    cdc_tables = [
        'cdc_bronze.br_customer_cdc',
        'cdc_bronze.br_payment_cdc', 
        'cdc_bronze.br_rental_cdc'
    ]
    
    try:
        with target_engine.connect() as conn:
            for table in cdc_tables:
                # CDC-specific validations
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                record_count = count_result.fetchone()[0]
                
                # Check LSN ordering (critical for CDC)
                lsn_check = conn.execute(text(f"""
                    SELECT 
                        COUNT(*) as total_lsns,
                        COUNT(DISTINCT lsn) as unique_lsns,
                        MAX(lsn) as max_lsn,
                        MIN(lsn) as min_lsn
                    FROM {table}
                """))
                lsn_row = lsn_check.fetchone()
                
                # Check JSONB payload validity
                payload_check = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM {table} 
                    WHERE full_row IS NOT NULL 
                    AND jsonb_typeof(full_row) = 'object'
                """))
                valid_payloads = payload_check.fetchone()[0]
                
                # Check operation types
                op_check = conn.execute(text(f"""
                    SELECT op, COUNT(*) 
                    FROM {table} 
                    GROUP BY op
                """))
                op_distribution = dict(op_check.fetchall())
                
                table_validation = {
                    "table": table,
                    "record_count": record_count,
                    "lsn_metrics": {
                        "total_lsns": int(lsn_row[0]) if lsn_row[0] else 0,
                        "unique_lsns": int(lsn_row[1]) if lsn_row[1] else 0,
                        "max_lsn": int(lsn_row[2]) if lsn_row[2] else 0,
                        "min_lsn": int(lsn_row[3]) if lsn_row[3] else 0
                    },
                    "valid_payloads": valid_payloads,
                    "operation_distribution": op_distribution,
                    "validation_passed": (
                        record_count > 0 and 
                        valid_payloads == record_count and
                        lsn_row[0] == lsn_row[1]  # LSNs should be unique
                    )
                }
                
                validation_results["cdc_table_validations"].append(table_validation)
                validation_results["total_cdc_records_validated"] += record_count
                
                if not table_validation["validation_passed"]:
                    validation_results["validation_errors"].append(
                        f"CDC table {table} failed validation: "
                        f"records={record_count}, valid_payloads={valid_payloads}"
                    )
        
        # Overall validation status
        validation_results["overall_success"] = len(validation_results["validation_errors"]) == 0
        
        return validation_results
        
    except Exception as e:
        validation_results["validation_errors"].append(f"CDC validation failed: {str(e)}")
        validation_results["overall_success"] = False
        return validation_results


def send_cdc_notification(**context) -> None:
    """Send real-time notification about CDC processing status"""
    # Get results from previous tasks - handle missing XCom gracefully
    ti = context['task_instance']
    ingestion_results = ti.xcom_pull(task_ids='run_cdc_transform', key='cdc_ingestion_results') or {}
    validation_results = ti.xcom_pull(task_ids='validate_cdc_data') or {}
    lag_results = ti.xcom_pull(task_ids='check_cdc_lag') or {}
    
    # Create CDC-specific summary
    summary = {
        "dag_id": DAG_ID,
        "execution_date": context['ds'],
        "batch_id": ingestion_results.get('batch_id', 'unknown'),
        "cdc_tables_processed": len(ingestion_results.get('cdc_tables_processed', [])),
        "total_cdc_rows": ingestion_results.get('total_cdc_rows_processed', 0),
        "validation_passed": validation_results.get('overall_success', False),
        "processing_errors": len(ingestion_results.get('errors', [])),
        "validation_errors": len(validation_results.get('validation_errors', [])),
        "high_lag_alert": lag_results.get('alert_required', False),
        "lag_tables": lag_results.get('high_lag_tables', [])
    }
    
    print(f"BRONZE CDC INGESTION SUMMARY: {summary}")
    
    # Alert if critical issues
    if summary["high_lag_alert"]:
        print(f"⚠️  CDC LAG ALERT: Tables with >10min lag: {summary['lag_tables']}")
    
    # In a real implementation, you would send this to real-time monitoring
    return summary


# Create the CDC DAG
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    catchup=False,
    tags=['bronze', 'cdc', 'pagila', 'real-time']
) as dag:
    
    # Start task
    start_task = DummyOperator(
        task_id='start_cdc_ingestion',
        doc_md="""
        ## Bronze CDC Pagila Ingestion Start
        
        Starting real-time CDC ingestion of Pagila changes into Bronze CDC layer.
        This DAG runs every 5 minutes and will:
        1. Check source and target database connectivity
        2. Monitor CDC lag for real-time alerting
        3. Extract incremental CDC changes from Pagila
        4. Transform to CDC format with LSN ordering
        5. Load incrementally with watermark management
        6. Validate CDC data quality and sequencing
        7. Send real-time notifications on lag/failures
        """
    )
    
    # Pre-flight checks with CDC lag monitoring
    with TaskGroup('preflight_checks') as preflight_group:
        
        check_source = PythonOperator(
            task_id='check_source_database',
            python_callable=check_source_connectivity,
            doc_md="Verify connectivity to source Pagila database"
        )
        
        check_target = PythonOperator(
            task_id='check_target_database', 
            python_callable=check_target_connectivity,
            doc_md="Verify connectivity to target Bronze CDC database"
        )
        
        check_lag = PythonOperator(
            task_id='check_cdc_lag',
            python_callable=check_cdc_lag,
            doc_md="Monitor CDC processing lag for real-time alerting"
        )
    
    # Main CDC ingestion transform
    run_transform = PythonOperator(
        task_id='run_cdc_transform',
        python_callable=run_cdc_ingestion,
        doc_md="""
        ## CDC Transform Execution
        
        Executes the main PySpark CDC transform job to:
        - Extract incremental changes based on watermarks
        - Convert to CDC format with LSN ordering and JSONB payloads
        - Manage LSN sequencing for proper ordering
        - Load data using incremental append strategy
        """
    )
    
    # CDC-specific data validation
    validate_data = PythonOperator(
        task_id='validate_cdc_data',
        python_callable=validate_cdc_data,
        doc_md="Validate CDC data for LSN ordering, payload integrity, and operation types"
    )
    
    # Real-time completion notification
    notify_completion = PythonOperator(
        task_id='send_cdc_notification',
        python_callable=send_cdc_notification,
        doc_md="Send real-time summary notification with CDC lag alerts"
    )
    
    # End task
    end_task = DummyOperator(
        task_id='end_cdc_ingestion',
        doc_md="Bronze CDC Pagila ingestion completed successfully"
    )
    
    # Define task dependencies
    start_task >> preflight_group >> run_transform >> validate_data >> notify_completion >> end_task
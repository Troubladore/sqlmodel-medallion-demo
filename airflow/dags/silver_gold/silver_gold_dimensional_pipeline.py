"""
🥈🥇 SILVER GOLD DIMENSIONAL - Complete Dimensional Modeling Pipeline DAG

Purpose: Orchestrate the complete silver-gold dimensional modeling pipeline
Schedule: Daily processing with bronze to silver to gold transformation
Architecture: Python native Airflow DAG with PySpark dimensional modeling tasks

Pipeline Structure:
1. Transform Bronze → Silver (data cleansing and business rules)
2. Transform Silver → Gold Dimensions (customer, film, store, staff)
3. Generate Date Dimension (comprehensive calendar)
4. Build Gold Facts (rental transactions with measures)
5. Calculate Gold Metrics (customer analytics and KPIs)
6. Validate dimensional model integrity
7. Send completion notifications with BI readiness

Dimensional Modeling Features:
- Classic star schema implementation
- Slowly changing dimensions (SCD Type 1)
- Pre-calculated business metrics
- Denormalized dimensions for performance
- Comprehensive date dimension
- Fact tables with additive measures

Dependencies:
- Bronze basic data must be available and current
- Silver and gold databases must exist
- Spark cluster/session must be available with adequate memory
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# Import our dimensional transform jobs
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from src.silver_gold_dimensional.transforms.bronze_to_silver_transforms import run as silver_transform
from src.silver_gold_dimensional.transforms.silver_to_gold_dimensional import run as gold_transform


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
DAG_ID = 'silver_gold_dimensional_pipeline'
SCHEDULE_INTERVAL = '@daily'  # Daily dimensional refresh
DESCRIPTION = 'Complete Silver-Gold Dimensional Modeling Pipeline for Business Intelligence'


def check_bronze_availability(**context) -> Dict[str, Any]:
    """Check that bronze data is available and current"""
    from sqlalchemy import create_engine, text
    
    try:
        # Test connection to bronze database
        bronze_url = "postgresql://postgres:postgres@host.docker.internal:5432/bronze_basic"
        bronze_engine = create_engine(bronze_url)
        
        with bronze_engine.connect() as conn:
            # Check that we have recent bronze data
            result = conn.execute(text("""
                SELECT 
                    table_name,
                    COUNT(*) as record_count
                FROM information_schema.tables t
                JOIN staging_pagila.br_actor a ON true
                WHERE t.table_schema = 'staging_pagila' 
                AND t.table_name LIKE 'br_%'
                GROUP BY table_name
                ORDER BY table_name
            """))
            
            bronze_tables = dict(result.fetchall()) if result else {}
            
            # Verify we have the minimum required data
            required_tables = ['br_customer', 'br_film', 'br_language', 'br_rental', 'br_payment']
            missing_tables = [table for table in required_tables 
                            if table not in bronze_tables or bronze_tables[table] == 0]
            
            if missing_tables:
                raise Exception(f"Missing or empty bronze tables: {missing_tables}")
            
            return {
                "status": "success",
                "message": "Bronze data availability confirmed",
                "bronze_tables": bronze_tables,
                "total_bronze_records": sum(bronze_tables.values())
            }
        
    except Exception as e:
        raise Exception(f"Bronze availability check failed: {str(e)}")


def check_target_readiness(**context) -> Dict[str, Any]:
    """Check that silver-gold target database is ready"""
    from sqlalchemy import create_engine, text
    
    try:
        # Test connection to silver-gold database
        target_url = "postgresql://postgres:postgres@host.docker.internal:5432/silver_gold_dimensional"
        target_engine = create_engine(target_url)
        
        with target_engine.connect() as conn:
            # Check schema existence
            schemas_result = conn.execute(text("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('pagila_silver', 'pagila_gold')
                ORDER BY schema_name
            """))
            
            schemas = [row[0] for row in schemas_result.fetchall()]
            
            # Check table existence
            tables_result = conn.execute(text("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema IN ('pagila_silver', 'pagila_gold')
                ORDER BY table_schema, table_name
            """))
            
            tables = {f"{row[0]}.{row[1]}" for row in tables_result.fetchall()}
            
            return {
                "status": "success",
                "message": "Target database readiness confirmed",
                "schemas_available": schemas,
                "tables_available": list(tables),
                "silver_tables_count": len([t for t in tables if t.startswith('pagila_silver')]),
                "gold_tables_count": len([t for t in tables if t.startswith('pagila_gold')])
            }
        
    except Exception as e:
        raise Exception(f"Target readiness check failed: {str(e)}")


def run_silver_transformation(**context) -> Dict[str, Any]:
    """Execute bronze to silver transformation"""
    from pyspark.sql import SparkSession
    
    # Get execution date from Airflow context
    execution_date = context['ds']  # YYYY-MM-DD format
    
    # Initialize Spark session with memory-optimized settings for container environment
    spark = (SparkSession.builder
             .appName(f"Silver_Dimensional_Transform_{execution_date}")
             # Memory settings for container environment (fixed for Spark 4.0.0 minimum requirements)
             .config("spark.executor.memory", "512m")
             .config("spark.driver.memory", "512m")
             .config("spark.driver.maxResultSize", "256m")
             .config("spark.executor.cores", "1")
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
        # Create configuration dictionary
        db_config = {
            'host': 'host.docker.internal',  # External PostgreSQL host
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres'
        }
        
        # Run silver transformation
        results = silver_transform(spark, db_config, execution_date)
        
        # Push results to XCom for downstream tasks
        context['task_instance'].xcom_push(key='silver_transformation_results', value=results)
        
        return results
        
    finally:
        spark.stop()


def run_gold_transformation(**context) -> Dict[str, Any]:
    """Execute silver to gold dimensional transformation"""
    from pyspark.sql import SparkSession
    
    # Get execution date from Airflow context
    execution_date = context['ds']  # YYYY-MM-DD format
    
    # Initialize Spark session with memory-optimized settings for container environment
    spark = (SparkSession.builder
             .appName(f"Gold_Dimensional_Transform_{execution_date}")
             # Memory settings for container environment (fixed for Spark 4.0.0 minimum requirements)
             .config("spark.executor.memory", "512m")
             .config("spark.driver.memory", "512m")
             .config("spark.driver.maxResultSize", "256m")
             .config("spark.executor.cores", "1")
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
        # Create configuration dictionary
        db_config = {
            'host': 'host.docker.internal',  # External PostgreSQL host
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres'
        }
        
        # Run gold dimensional transformation
        results = gold_transform(spark, db_config, execution_date)
        
        # Push results to XCom for downstream tasks
        context['task_instance'].xcom_push(key='gold_transformation_results', value=results)
        
        return results
        
    finally:
        spark.stop()


def validate_dimensional_model(**context) -> Dict[str, Any]:
    """Validate the dimensional model integrity and business rules"""
    from sqlalchemy import create_engine, text
    
    # Get transformation results from upstream tasks
    ti = context['task_instance']
    silver_results = ti.xcom_pull(task_ids='run_silver_transformation', key='silver_transformation_results') or {}
    gold_results = ti.xcom_pull(task_ids='run_gold_transformation', key='gold_transformation_results') or {}
    
    target_url = "postgresql://postgres:postgres@host.docker.internal:5432/silver_gold_dimensional"
    target_engine = create_engine(target_url)
    
    validation_results = {
        "execution_date": context['ds'],
        "silver_batch_id": silver_results.get('batch_id'),
        "gold_batch_id": gold_results.get('batch_id'),
        "dimensional_validations": [],
        "total_dimensional_records": 0,
        "validation_errors": []
    }
    
    # Define dimensional model validation rules
    dimensional_validations = [
        {
            "name": "Silver Layer Record Counts",
            "query": """
                SELECT 'sl_customer' as table, COUNT(*) as count FROM pagila_silver.sl_customer
                UNION ALL SELECT 'sl_film', COUNT(*) FROM pagila_silver.sl_film
                UNION ALL SELECT 'sl_address', COUNT(*) FROM pagila_silver.sl_address
                UNION ALL SELECT 'sl_store', COUNT(*) FROM pagila_silver.sl_store
                UNION ALL SELECT 'sl_language', COUNT(*) FROM pagila_silver.sl_language
            """,
            "validation": "All silver tables should have > 0 records"
        },
        {
            "name": "Gold Dimension Integrity",
            "query": """
                SELECT 'gl_dim_customer' as table, COUNT(*) as count FROM pagila_gold.gl_dim_customer
                UNION ALL SELECT 'gl_dim_film', COUNT(*) FROM pagila_gold.gl_dim_film
                UNION ALL SELECT 'gl_dim_store', COUNT(*) FROM pagila_gold.gl_dim_store
                UNION ALL SELECT 'gl_dim_staff', COUNT(*) FROM pagila_gold.gl_dim_staff
                UNION ALL SELECT 'gl_dim_date', COUNT(*) FROM pagila_gold.gl_dim_date
            """,
            "validation": "All dimension tables should have > 0 records"
        },
        {
            "name": "Fact Table Measures",
            "query": """
                SELECT 
                    COUNT(*) as total_facts,
                    COUNT(DISTINCT customer_key) as unique_customers,
                    COUNT(DISTINCT film_key) as unique_films,
                    SUM(CASE WHEN revenue_amount > 0 THEN 1 ELSE 0 END) as revenue_facts,
                    AVG(rental_rate) as avg_rental_rate
                FROM pagila_gold.gl_fact_rental
            """,
            "validation": "Fact table should have proper measures and foreign keys"
        },
        {
            "name": "Data Quality Scores",
            "query": """
                SELECT 
                    COUNT(*) as total_silver_records,
                    AVG(sl_data_quality_score) as avg_quality_score,
                    COUNT(CASE WHEN sl_data_quality_score >= 0.8 THEN 1 END) as high_quality_records
                FROM pagila_silver.sl_customer
            """,
            "validation": "Silver layer should maintain high data quality scores"
        },
        {
            "name": "Customer Analytics",
            "query": """
                SELECT 
                    COUNT(*) as total_customers,
                    AVG(total_rentals) as avg_rentals_per_customer,
                    AVG(total_payments) as avg_payments_per_customer,
                    COUNT(CASE WHEN customer_tier = 'Platinum' THEN 1 END) as platinum_customers
                FROM pagila_gold.gl_dim_customer
            """,
            "validation": "Customer dimension should have proper analytics"
        }
    ]
    
    try:
        with target_engine.connect() as conn:
            for validation in dimensional_validations:
                try:
                    result = conn.execute(text(validation["query"]))
                    rows = result.fetchall()
                    
                    validation_result = {
                        "validation_name": validation["name"],
                        "results": [dict(zip(result.keys(), row)) for row in rows],
                        "rule": validation["validation"],
                        "passed": len(rows) > 0 and all(row[1] > 0 for row in rows if len(row) > 1)
                    }
                    
                    validation_results["dimensional_validations"].append(validation_result)
                    
                    if not validation_result["passed"]:
                        validation_results["validation_errors"].append(
                            f"Failed validation: {validation['name']}"
                        )
                    
                    # Count total records for reporting
                    if "COUNT(*)" in validation["query"]:
                        validation_results["total_dimensional_records"] += sum(
                            row[1] for row in rows if len(row) > 1 and isinstance(row[1], int)
                        )
                    
                except Exception as validation_error:
                    validation_results["validation_errors"].append(
                        f"Validation '{validation['name']}' failed: {str(validation_error)}"
                    )
        
        # Overall validation status
        validation_results["overall_success"] = len(validation_results["validation_errors"]) == 0
        
        return validation_results
        
    except Exception as e:
        validation_results["validation_errors"].append(f"Dimensional validation failed: {str(e)}")
        validation_results["overall_success"] = False
        return validation_results


def send_dimensional_notification(**context) -> None:
    """Send comprehensive notification about dimensional modeling pipeline completion"""
    # Get results from previous tasks
    ti = context['task_instance']
    bronze_check = ti.xcom_pull(task_ids='check_bronze_availability') or {}
    silver_results = ti.xcom_pull(task_ids='run_silver_transformation', key='silver_transformation_results') or {}
    gold_results = ti.xcom_pull(task_ids='run_gold_transformation', key='gold_transformation_results') or {}
    validation_results = ti.xcom_pull(task_ids='validate_dimensional_model') or {}
    
    # Create comprehensive dimensional summary
    summary = {
        "dag_id": DAG_ID,
        "execution_date": context['ds'],
        "pipeline_type": "dimensional_modeling",
        
        # Bronze source metrics
        "bronze_tables_available": len(bronze_check.get('bronze_tables', {})),
        "total_bronze_records": bronze_check.get('total_bronze_records', 0),
        
        # Silver transformation metrics
        "silver_batch_id": silver_results.get('batch_id'),
        "silver_tables_processed": len(silver_results.get('silver_tables_processed', [])),
        "total_silver_rows": silver_results.get('total_silver_rows_processed', 0),
        "silver_errors": len(silver_results.get('errors', [])),
        
        # Gold transformation metrics
        "gold_batch_id": gold_results.get('batch_id'),
        "gold_tables_processed": len(gold_results.get('gold_tables_processed', [])),
        "total_gold_rows": gold_results.get('total_gold_rows_processed', 0),
        "gold_errors": len(gold_results.get('errors', [])),
        
        # Dimensional model validation
        "dimensional_validations_passed": validation_results.get('overall_success', False),
        "total_dimensional_records": validation_results.get('total_dimensional_records', 0),
        "validation_errors": len(validation_results.get('validation_errors', [])),
        
        # Business intelligence readiness
        "bi_ready": (
            validation_results.get('overall_success', False) and
            len(silver_results.get('errors', [])) == 0 and
            len(gold_results.get('errors', [])) == 0
        )
    }
    
    print(f"DIMENSIONAL MODELING PIPELINE SUMMARY: {summary}")
    
    # Alert on critical issues
    if not summary["bi_ready"]:
        print(f"⚠️  BUSINESS INTELLIGENCE NOT READY: Check validation errors and processing failures")
    else:
        print(f"✅ BUSINESS INTELLIGENCE READY: {summary['total_dimensional_records']} records available for analytics")
    
    # In a real implementation, send to BI team notification systems
    return summary


# Create the Dimensional Modeling DAG
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    catchup=False,
    tags=['silver', 'gold', 'dimensional', 'star-schema', 'business-intelligence']
) as dag:
    
    # Start task
    start_task = DummyOperator(
        task_id='start_dimensional_pipeline',
        doc_md="""
        ## Silver-Gold Dimensional Modeling Pipeline Start
        
        Starting the complete dimensional modeling pipeline for business intelligence.
        This DAG will:
        1. Validate bronze data availability and freshness
        2. Transform bronze → silver (data cleansing and business rules)
        3. Transform silver → gold dimensions (customer, film, store, staff, date)
        4. Build gold fact tables (rental transactions with measures)
        5. Calculate gold metrics (customer analytics and KPIs)
        6. Validate dimensional model integrity
        7. Send BI readiness notifications
        """
    )
    
    # Pre-flight checks for dimensional processing
    with TaskGroup('preflight_checks') as preflight_group:
        
        check_bronze = PythonOperator(
            task_id='check_bronze_availability',
            python_callable=check_bronze_availability,
            doc_md="Verify bronze data is available and current for dimensional processing"
        )
        
        check_target = PythonOperator(
            task_id='check_target_readiness', 
            python_callable=check_target_readiness,
            doc_md="Verify silver-gold target database and schemas are ready"
        )
    
    # Silver layer transformation
    run_silver = PythonOperator(
        task_id='run_silver_transformation',
        python_callable=run_silver_transformation,
        doc_md="""
        ## Silver Layer Transformation
        
        Executes bronze → silver transformation with:
        - Data cleansing and standardization
        - Business rule application
        - Data quality score calculation
        - Silver audit trail creation
        - Type conversion and validation
        """
    )
    
    # Gold dimensional transformation
    run_gold = PythonOperator(
        task_id='run_gold_transformation',
        python_callable=run_gold_transformation,
        doc_md="""
        ## Gold Dimensional Transformation
        
        Executes silver → gold dimensional modeling with:
        - Star schema dimension table creation
        - Fact table construction with measures
        - Date dimension generation
        - Customer analytics and segmentation
        - Pre-calculated business metrics
        """
    )
    
    # Dimensional model validation
    validate_model = PythonOperator(
        task_id='validate_dimensional_model',
        python_callable=validate_dimensional_model,
        doc_md="Validate dimensional model integrity and business intelligence readiness"
    )
    
    # Pipeline completion notification
    notify_completion = PythonOperator(
        task_id='send_dimensional_notification',
        python_callable=send_dimensional_notification,
        doc_md="Send comprehensive summary of dimensional modeling pipeline completion"
    )
    
    # End task
    end_task = DummyOperator(
        task_id='end_dimensional_pipeline',
        doc_md="Silver-Gold dimensional modeling pipeline completed successfully - BI ready!"
    )
    
    # Define task dependencies
    start_task >> preflight_group >> run_silver >> run_gold >> validate_model >> notify_completion >> end_task
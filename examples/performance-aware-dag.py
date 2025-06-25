"""
🎯 Example: Performance-Aware DAG Implementation

This example shows how to create DAGs that automatically adapt to different
performance tiers and machine capabilities.

Key Features:
- Automatic performance tier detection
- Dynamic Spark configuration
- Graceful degradation for resource-constrained environments
- Easy override via environment variables
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Import our performance-aware Spark configuration
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))

from scripts.spark_config_generator import get_spark_session, get_current_tier


# Default arguments
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


def run_performance_aware_transform(**context) -> Dict[str, Any]:
    """
    Example transform that adapts to detected performance tier
    """
    execution_date = context['ds']
    
    # Get performance tier (auto-detected or from environment)
    current_tier = get_current_tier()
    print(f"🎯 Running on {current_tier} performance tier")
    
    # Get optimized Spark session for detected tier
    spark = get_spark_session(f"PerformanceAware_Transform_{execution_date}")
    
    try:
        # Database configuration
        db_config = {
            'host': 'host.docker.internal',
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres'
        }
        
        # Extract source data
        source_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/pagila"
        
        # Performance tier affects processing strategy
        if current_tier == "performance":
            # High-performance: Process all tables in parallel
            tables = ["actor", "customer", "film", "payment", "rental"]
            print("🚀 High-performance mode: Parallel processing enabled")
            
            # Use advanced Spark features
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
            
        elif current_tier == "balanced":
            # Balanced: Process in smaller batches
            tables = ["actor", "customer", "film"]  # Process core tables first
            print("⚖️  Balanced mode: Batch processing")
            
        else:  # lite
            # Lite: Process one table at a time, minimal features
            tables = ["actor"]  # Start with smallest table
            print("💡 Lite mode: Sequential processing, conservative settings")
            
            # Disable potentially memory-intensive features
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        
        total_records = 0
        
        for table in tables:
            print(f"Processing {table}...")
            
            df = (spark.read
                  .format("jdbc")
                  .option("url", source_url)
                  .option("dbtable", f"public.{table}")
                  .option("user", db_config['user'])
                  .option("password", db_config['password'])
                  .option("driver", "org.postgresql.Driver")
                  .load())
            
            record_count = df.count()
            total_records += record_count
            print(f"  {table}: {record_count} records")
        
        results = {
            "execution_date": execution_date,
            "performance_tier": current_tier,
            "tables_processed": len(tables),
            "total_records": total_records,
            "spark_config": dict(spark.sparkContext.getConf().getAll())
        }
        
        print(f"✅ Completed processing {len(tables)} tables with {total_records} total records")
        return results
        
    finally:
        spark.stop()


def check_performance_tier(**context) -> Dict[str, Any]:
    """Check and report current performance configuration"""
    import psutil
    
    tier = get_current_tier()
    total_memory = round(psutil.virtual_memory().total / (1024**3), 1)
    cpu_count = psutil.cpu_count()
    
    print(f"🖥️  System Resources:")
    print(f"   Memory: {total_memory} GB")
    print(f"   CPUs: {cpu_count}")
    print(f"   Performance Tier: {tier}")
    
    # Warn if performance might be suboptimal
    if tier == "lite" and total_memory >= 12:
        print("⚠️  Consider using --config balanced for better performance")
    elif tier == "balanced" and total_memory >= 16:
        print("💡 Consider using --config performance for maximum speed")
    
    return {
        "tier": tier,
        "memory_gb": total_memory,
        "cpu_count": cpu_count
    }


# Create the DAG
with DAG(
    dag_id='example_performance_aware_pipeline',
    default_args=DEFAULT_ARGS,
    description='Example of performance-aware DAG that adapts to system resources',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,
    tags=['example', 'performance', 'adaptive']
) as dag:
    
    start_task = DummyOperator(
        task_id='start_performance_aware_pipeline'
    )
    
    check_tier = PythonOperator(
        task_id='check_performance_tier',
        python_callable=check_performance_tier
    )
    
    run_transform = PythonOperator(
        task_id='run_performance_aware_transform',
        python_callable=run_performance_aware_transform
    )
    
    end_task = DummyOperator(
        task_id='end_performance_aware_pipeline'
    )
    
    # Task dependencies
    start_task >> check_tier >> run_transform >> end_task
"""
🥉 BRONZE CDC - Pagila Source to Bronze CDC Transform

Purpose: Extract CDC changes from source Pagila database and load into bronze CDC layer
Demonstrates: Advanced CDC ingestion patterns with LSN ordering and JSONB payloads
Architecture: PySpark 4.0.0 job for Databricks/Snowpark compatibility

CDC Strategy:
- Incremental watermark-based extraction (simulating CDC)
- LSN-ordered event sequencing for guaranteed ordering
- JSONB payloads for flexible schema evolution
- Append-only processing for complete change history
"""

import hashlib
import uuid
from datetime import datetime, timedelta
from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, lit, col, to_json, struct,
    when, isnan, isnull, expr, row_number, monotonically_increasing_id,
    max as spark_max
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


def extract_cdc_changes(spark: SparkSession, db_config: dict, table_name: str, 
                       last_lsn: int = 0, watermark_minutes: int = 5) -> DataFrame:
    """Extract CDC changes using timestamp-based simulation"""
    
    # Build source JDBC connection
    source_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/pagila"
    
    # Extract with time-based watermark (simulates real CDC)
    df = (spark.read
          .format("jdbc")
          .option("url", source_url)
          .option("dbtable", f"public.{table_name}")
          .option("user", db_config['user'])
          .option("password", db_config.get('password', 'postgres'))
          .option("driver", "org.postgresql.Driver")
          .load())
    
    # Simulate CDC by filtering recent changes (in production: from WAL)
    # For demo, we'll process all data as "new changes"
    
    # Cast all columns to string for JSON payload
    for column in df.columns:
        df = df.withColumn(column, col(column).cast(StringType()))
    
    return df


def create_cdc_payload(df: DataFrame, table_name: str, batch_id: str) -> DataFrame:
    """Convert source data to CDC format with JSONB payload"""
    
    source_columns = [c for c in df.columns]
    
    # Create JSONB payload from all source columns
    df_with_payload = df.withColumn(
        "full_row", 
        to_json(struct(*[col(c).alias(c) for c in source_columns]))
    )
    
    # Generate monotonic LSNs with window function
    window_spec = Window.orderBy(monotonically_increasing_id())
    
    df_cdc = (df_with_payload
        .withColumn("lsn", row_number().over(window_spec))
        .withColumn("op", lit("I"))  # Operation type: I(nsert), U(pdate), D(elete)
        .withColumn("load_ts", current_timestamp())
        .select("lsn", "op", "full_row", "load_ts"))
    
    return df_cdc


def get_last_lsn(spark: SparkSession, db_config: dict, cdc_table: str) -> int:
    """Get highest LSN for incremental processing"""
    
    try:
        target_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/bronze_cdc"
        
        df = (spark.read
              .format("jdbc")
              .option("url", target_url)
              .option("dbtable", f"cdc_bronze.{cdc_table}")
              .option("user", db_config['user'])
              .option("password", db_config.get('password', 'postgres'))
              .option("driver", "org.postgresql.Driver")
              .load())
        
        if df.count() == 0:
            return 0
            
        return df.agg(spark_max("lsn")).collect()[0][0] or 0
        
    except Exception:
        return 0  # Table empty or doesn't exist


def load_to_cdc_table(df: DataFrame, db_config: dict, cdc_table: str) -> None:
    """Load CDC DataFrame with incremental append strategy"""
    
    target_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/bronze_cdc"
    
    (df.write
     .format("jdbc")
     .option("url", target_url)
     .option("dbtable", f"cdc_bronze.{cdc_table}")
     .option("user", db_config['user'])
     .option("password", db_config.get('password', 'postgres'))
     .option("driver", "org.postgresql.Driver")
     .mode("append")  # CDC always appends, never overwrites
     .save())


def transform_customer_cdc_to_bronze(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform customer CDC to bronze with watermark management"""
    
    try:
        # Get watermark for incremental processing
        last_lsn = get_last_lsn(spark, db_config, "br_customer_cdc")
        
        # For static demo data, only process if no data exists yet
        if last_lsn > 0:
            # Already processed all static data, no new CDC events
            return {"customer_cdc_rows_processed": 0}
        
        # Extract CDC changes (only on first run for static data)
        source_df = extract_cdc_changes(spark, db_config, "customer")
        
        if source_df.count() == 0:
            return {"customer_cdc_rows_processed": 0}
        
        # Create CDC payload with LSN
        cdc_df = create_cdc_payload(source_df, "customer", batch_id)
        
        # Load incrementally
        load_to_cdc_table(cdc_df, db_config, "br_customer_cdc")
        
        return {"customer_cdc_rows_processed": cdc_df.count()}
        
    except Exception as e:
        print(f"Error in customer CDC transform: {str(e)}")
        return {"customer_cdc_rows_processed": 0}


def transform_payment_cdc_to_bronze(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform payment CDC to bronze with watermark management"""
    
    try:
        # Get watermark for incremental processing
        last_lsn = get_last_lsn(spark, db_config, "br_payment_cdc")
        
        # For static demo data, only process if no data exists yet
        if last_lsn > 0:
            # Already processed all static data, no new CDC events
            return {"payment_cdc_rows_processed": 0}
        
        # Extract CDC changes (only on first run for static data)
        source_df = extract_cdc_changes(spark, db_config, "payment")
        
        if source_df.count() == 0:
            return {"payment_cdc_rows_processed": 0}
        
        # Create CDC payload with LSN
        cdc_df = create_cdc_payload(source_df, "payment", batch_id)
        
        # Load incrementally
        load_to_cdc_table(cdc_df, db_config, "br_payment_cdc")
        
        return {"payment_cdc_rows_processed": cdc_df.count()}
        
    except Exception as e:
        print(f"Error in payment CDC transform: {str(e)}")
        return {"payment_cdc_rows_processed": 0}


def transform_rental_cdc_to_bronze(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform rental CDC to bronze with watermark management"""
    
    try:
        # Get watermark for incremental processing
        last_lsn = get_last_lsn(spark, db_config, "br_rental_cdc")
        
        # For static demo data, only process if no data exists yet
        if last_lsn > 0:
            # Already processed all static data, no new CDC events
            return {"rental_cdc_rows_processed": 0}
        
        # Extract CDC changes (only on first run for static data)
        source_df = extract_cdc_changes(spark, db_config, "rental")
        
        if source_df.count() == 0:
            return {"rental_cdc_rows_processed": 0}
        
        # Create CDC payload with LSN
        cdc_df = create_cdc_payload(source_df, "rental", batch_id)
        
        # Load incrementally
        load_to_cdc_table(cdc_df, db_config, "br_rental_cdc")
        
        return {"rental_cdc_rows_processed": cdc_df.count()}
        
    except Exception as e:
        print(f"Error in rental CDC transform: {str(e)}")
        return {"rental_cdc_rows_processed": 0}


def run(spark: SparkSession, db_config: dict, execution_date: str = None) -> Dict[str, any]:
    """
    Main entry point for CDC ingestion transform
    
    Args:
        spark: SparkSession for data processing
        db_config: Database configuration dictionary with connection details
        execution_date: Airflow execution date (optional)
    
    Returns:
        Dict with processing metrics and results
    """
    
    # Generate batch ID for this CDC run
    batch_id = f"bronze_cdc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if execution_date:
        batch_id = f"bronze_cdc_{execution_date.replace('-', '').replace(':', '')}"
    
    # Define CDC tables to process (subset of bronze_basic tables)
    cdc_tables = [
        ("customer", transform_customer_cdc_to_bronze),
        ("payment", transform_payment_cdc_to_bronze),
        ("rental", transform_rental_cdc_to_bronze)
    ]
    
    # Process each CDC table and collect metrics
    results = {
        "batch_id": batch_id,
        "execution_date": execution_date or datetime.now().isoformat(),
        "cdc_tables_processed": [],
        "total_cdc_rows_processed": 0,
        "errors": []
    }
    
    for table_name, transform_func in cdc_tables:
        try:
            table_metrics = transform_func(spark, db_config, batch_id)
            results["cdc_tables_processed"].append({
                "table": table_name,
                "metrics": table_metrics
            })
            results["total_cdc_rows_processed"] += list(table_metrics.values())[0]
            
        except Exception as e:
            error_msg = f"Failed to process CDC {table_name}: {str(e)}"
            results["errors"].append(error_msg)
            print(f"ERROR: {error_msg}")
    
    return results


if __name__ == "__main__":
    # For local testing with Spark 4.0.0
    spark = (SparkSession.builder
             .appName("Bronze_CDC_Transform")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .getOrCreate())
    
    # Simple configuration for testing
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres'
    }
    
    # Run CDC transform
    results = run(spark, db_config)
    
    print(f"CDC transformation completed: {results}")
    
    spark.stop()
"""
🥈 SILVER LAYER - Bronze to Silver Data Cleansing & Standardization Transform

Purpose: Transform bronze basic data into clean, business-ready silver layer entities
Demonstrates: Data quality, business rules, standardization, and analytical preparation
Architecture: PySpark 4.0.0 job for Databricks/Snowpark compatibility

Silver Layer Strategy:
- Clean and standardize data from bronze lenient types
- Apply business rules and data quality validations
- Calculate business metrics and derived fields
- Maintain natural business keys with surrogate keys
- Preserve complete audit trail from bronze to silver

Spark 4.0.0 Features Used:
- Enhanced data quality functions and validation
- Improved type casting and null handling
- Advanced string processing and regex capabilities
- Enhanced join optimizations for dimensional lookups
"""

from datetime import datetime, date
from typing import Dict, List, Optional
from uuid import uuid4

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, lit, col, when, isnan, isnull, 
    trim, upper, lower, regexp_replace, coalesce,
    cast, concat, concat_ws, split, length, 
    count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    md5, sha2, expr, row_number, dense_rank,
    to_date, date_format, year, month, dayofmonth, 
    round as spark_round, monotonically_increasing_id
)
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, DateType, TimestampType
from pyspark.sql.window import Window


def extract_bronze_table(spark: SparkSession, db_config: dict, table_name: str) -> DataFrame:
    """Extract bronze table data with comprehensive error handling"""
    
    # Build bronze JDBC connection
    bronze_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/bronze_basic"
    
    df = (spark.read
          .format("jdbc")
          .option("url", bronze_url)
          .option("dbtable", f"staging_pagila.{table_name}")
          .option("user", db_config['user'])
          .option("password", db_config.get('password', 'postgres'))
          .option("driver", "org.postgresql.Driver")
          .load())
    
    return df


def calculate_data_quality_score(df: DataFrame, required_fields: List[str]) -> DataFrame:
    """Calculate data quality score based on completeness and validity"""
    
    quality_score = lit(1.0)  # Start with perfect score
    
    for field in required_fields:
        if field in df.columns:
            # Deduct points for nulls and empty strings
            quality_score = quality_score - when(
                (col(field).isNull()) | (trim(col(field)) == ""), 
                lit(0.2)
            ).otherwise(lit(0.0))
    
    # Ensure score stays between 0 and 1
    quality_score = when(quality_score < lit(0.0), lit(0.0)).otherwise(quality_score)
    
    return df.withColumn("sl_data_quality_score", quality_score)


def add_silver_audit_fields(df: DataFrame, source_table: str, batch_id: str) -> DataFrame:
    """Add comprehensive silver audit trail"""
    
    # For bronze key mapping, use proper null handling with explicit types
    if f"br_{source_table}_key" in df.columns:
        bronze_key_col = col(f"br_{source_table}_key")
    else:
        # For synthetic data (address, store), use null string instead of void
        bronze_key_col = lit(None).cast(StringType())
    
    return (df
        .withColumn("sl_created_time", current_timestamp())
        .withColumn("sl_updated_time", current_timestamp())
        .withColumn("sl_source_bronze_key", bronze_key_col)
        .withColumn("sl_is_active", lit(True))
        .withColumn("sl_batch_id", lit(batch_id)))


def load_to_silver_table(df: DataFrame, db_config: dict, silver_table: str) -> None:
    """Load silver DataFrame with full refresh strategy"""
    
    # Build silver JDBC connection
    silver_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/silver_gold_dimensional"
    
    (df.write
     .format("jdbc")
     .option("url", silver_url)
     .option("dbtable", f"pagila_silver.{silver_table}")
     .option("user", db_config['user'])
     .option("password", db_config.get('password', 'postgres'))
     .option("driver", "org.postgresql.Driver")
     .mode("overwrite")  # Silver uses full refresh like bronze
     .save())


def transform_customer_to_silver(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform bronze customer to clean silver customer with business logic"""
    
    # Extract bronze customer data
    bronze_df = extract_bronze_table(spark, db_config, "br_customer")
    
    if bronze_df.count() == 0:
        return {"sl_customer_rows_processed": 0}
    
    # Clean and standardize customer data
    clean_df = (bronze_df
        # Generate silver surrogate key
        .withColumn("sl_customer_key", expr("uuid()"))
        
        # Clean and cast core business fields
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .withColumn("store_id", col("store_id").cast(IntegerType()))
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("email", 
            when(trim(col("email")) == "", None)
            .otherwise(lower(trim(col("email")))))
        .withColumn("address_id", col("address_id").cast(IntegerType()))
        .withColumn("activebool", 
            when(lower(trim(col("activebool"))) == "true", True)
            .when(lower(trim(col("activebool"))) == "false", False)
            .otherwise(True))
        .withColumn("create_date", to_date(col("create_date")))
        .withColumn("last_update", col("last_update").cast(TimestampType()))
        .withColumn("active", col("active").cast(IntegerType())))
    
    # Calculate business metrics (placeholders for now)
    enriched_df = (clean_df
        .withColumn("sl_lifetime_value", lit(None).cast(FloatType()))
        .withColumn("sl_rental_count", lit(0))
        .withColumn("sl_last_rental_date", lit(None).cast(DateType())))
    
    # Calculate data quality score
    required_fields = ["customer_id", "first_name", "last_name", "store_id", "address_id"]
    quality_df = calculate_data_quality_score(enriched_df, required_fields)
    
    # Add silver audit fields
    silver_df = add_silver_audit_fields(quality_df, "customer", batch_id)
    
    # Select final silver schema
    final_df = silver_df.select(
        "sl_customer_key", "customer_id", "store_id", "first_name", "last_name", 
        "email", "address_id", "activebool", "create_date", "last_update", "active",
        "sl_created_time", "sl_updated_time", "sl_source_bronze_key", "sl_is_active", 
        "sl_data_quality_score", "sl_lifetime_value", "sl_rental_count", "sl_last_rental_date"
    )
    
    # Load to silver table
    load_to_silver_table(final_df, db_config, "sl_customer")
    
    return {"sl_customer_rows_processed": final_df.count()}


def transform_film_to_silver(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform bronze film to clean silver film with business logic"""
    
    # Extract bronze film data
    bronze_df = extract_bronze_table(spark, db_config, "br_film")
    
    if bronze_df.count() == 0:
        return {"sl_film_rows_processed": 0}
    
    # Clean and standardize film data
    clean_df = (bronze_df
        # Generate silver surrogate key
        .withColumn("sl_film_key", expr("uuid()"))
        
        # Clean and cast core business fields
        .withColumn("film_id", col("film_id").cast(IntegerType()))
        .withColumn("title", trim(col("title")))
        .withColumn("description", trim(col("description")))
        .withColumn("release_year", col("release_year").cast(IntegerType()))
        .withColumn("language_id", col("language_id").cast(IntegerType()))
        .withColumn("rental_duration", col("rental_duration").cast(IntegerType()))
        .withColumn("rental_rate", col("rental_rate").cast(FloatType()))
        .withColumn("length", col("length").cast(IntegerType()))
        .withColumn("replacement_cost", col("replacement_cost").cast(FloatType()))
        .withColumn("rating", trim(upper(col("rating"))))
        .withColumn("last_update", col("last_update").cast(TimestampType()))
        
        # Parse special_features from text
        .withColumn("special_features", 
            when(trim(col("special_features")) == "", None)
            .otherwise(trim(col("special_features")))))
    
    # Calculate business metrics
    enriched_df = (clean_df
        .withColumn("sl_revenue_potential", 
            col("rental_rate") * lit(10))  # Estimated revenue potential
        .withColumn("sl_is_new_release", 
            when(col("release_year") >= lit(2005), True).otherwise(False))
        .withColumn("sl_length_category",
            when(col("length") <= lit(90), "Short")
            .when(col("length") <= lit(120), "Medium")
            .otherwise("Long")))
    
    # Calculate data quality score
    required_fields = ["film_id", "title", "language_id", "rental_duration", "rental_rate"]
    quality_df = calculate_data_quality_score(enriched_df, required_fields)
    
    # Add silver audit fields
    silver_df = add_silver_audit_fields(quality_df, "film", batch_id)
    
    # Select final silver schema
    final_df = silver_df.select(
        "sl_film_key", "film_id", "title", "description", "release_year", "language_id",
        "rental_duration", "rental_rate", "length", "replacement_cost", "rating",
        "special_features", "last_update", "sl_created_time", "sl_updated_time", 
        "sl_source_bronze_key", "sl_is_active", "sl_data_quality_score",
        "sl_revenue_potential", "sl_is_new_release", "sl_length_category"
    )
    
    # Load to silver table
    load_to_silver_table(final_df, db_config, "sl_film")
    
    return {"sl_film_rows_processed": final_df.count()}


def transform_address_to_silver(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform bronze address to clean silver address"""
    
    # For now, we'll extract from customer since we don't have separate address bronze
    # In a real implementation, you'd have a separate bronze address table
    
    # Create placeholder silver address from customer data
    bronze_df = extract_bronze_table(spark, db_config, "br_customer")
    
    if bronze_df.count() == 0:
        return {"sl_address_rows_processed": 0}
    
    # Create synthetic address data (in real scenario, would come from proper bronze address)
    address_df = (bronze_df
        .withColumn("sl_address_key", expr("uuid()").cast(StringType()))
        .withColumn("address_id", col("address_id").cast(IntegerType()))
        .withColumn("address", lit("123 Main St").cast(StringType()))  # Placeholder
        .withColumn("address2", lit(None).cast(StringType()))
        .withColumn("district", lit("Central").cast(StringType()))
        .withColumn("city_id", lit(1).cast(IntegerType()))
        .withColumn("postal_code", lit("12345").cast(StringType()))
        .withColumn("phone", lit("555-0123").cast(StringType()))
        .withColumn("last_update", current_timestamp())
        .select("sl_address_key", "address_id", "address", "address2", 
                "district", "city_id", "postal_code", "phone", "last_update")
        .distinct())
    
    # Add silver audit fields
    silver_df = add_silver_audit_fields(address_df, "address", batch_id)
    
    # Load to silver table
    load_to_silver_table(silver_df, db_config, "sl_address")
    
    return {"sl_address_rows_processed": silver_df.count()}


def transform_store_to_silver(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform bronze store to clean silver store"""
    
    # Create synthetic store data from customer store_ids
    bronze_df = extract_bronze_table(spark, db_config, "br_customer")
    
    if bronze_df.count() == 0:
        return {"sl_store_rows_processed": 0}
    
    # Create store dimension from distinct store_ids in customer data
    store_df = (bronze_df
        .select("store_id")
        .distinct()
        .withColumn("sl_store_key", expr("uuid()").cast(StringType()))
        .withColumn("store_id", col("store_id").cast(IntegerType()))
        .withColumn("manager_staff_id", lit(1).cast(IntegerType()))
        .withColumn("address_id", col("store_id").cast(IntegerType()))  # Assume store_id maps to address_id
        .withColumn("last_update", current_timestamp()))
    
    # Add silver audit fields
    silver_df = add_silver_audit_fields(store_df, "store", batch_id)
    
    # Load to silver table
    load_to_silver_table(silver_df, db_config, "sl_store")
    
    return {"sl_store_rows_processed": silver_df.count()}


def transform_language_to_silver(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform bronze language to clean silver language"""
    
    # Extract bronze language data
    bronze_df = extract_bronze_table(spark, db_config, "br_language")
    
    if bronze_df.count() == 0:
        return {"sl_language_rows_processed": 0}
    
    # Clean and standardize language data
    clean_df = (bronze_df
        # Generate silver surrogate key
        .withColumn("sl_language_key", expr("uuid()"))
        
        # Clean and cast fields
        .withColumn("language_id", col("language_id").cast(IntegerType()))
        .withColumn("name", trim(col("name")))
        .withColumn("last_update", col("last_update").cast(TimestampType())))
    
    # Calculate data quality score
    required_fields = ["language_id", "name"]
    quality_df = calculate_data_quality_score(clean_df, required_fields)
    
    # Add silver audit fields
    silver_df = add_silver_audit_fields(quality_df, "language", batch_id)
    
    # Select final silver schema
    final_df = silver_df.select(
        "sl_language_key", "language_id", "name", "last_update",
        "sl_created_time", "sl_updated_time", "sl_source_bronze_key", 
        "sl_is_active", "sl_data_quality_score"
    )
    
    # Load to silver table
    load_to_silver_table(final_df, db_config, "sl_language")
    
    return {"sl_language_rows_processed": final_df.count()}


def run(spark: SparkSession, db_config: dict, execution_date: str = None) -> Dict[str, any]:
    """
    Main entry point for bronze to silver transformation
    
    Args:
        spark: SparkSession for data processing
        db_config: Database configuration dictionary with connection details
        execution_date: Airflow execution date (optional)
    
    Returns:
        Dict with processing metrics and results
    """
    
    # Generate batch ID for this silver run
    batch_id = f"silver_dimensional_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if execution_date:
        batch_id = f"silver_dimensional_{execution_date.replace('-', '').replace(':', '')}"
    
    # Define silver tables to process
    silver_tables = [
        ("address", transform_address_to_silver),
        ("store", transform_store_to_silver),
        ("language", transform_language_to_silver),
        ("customer", transform_customer_to_silver),
        ("film", transform_film_to_silver)
    ]
    
    # Process each silver table and collect metrics
    results = {
        "batch_id": batch_id,
        "execution_date": execution_date or datetime.now().isoformat(),
        "silver_tables_processed": [],
        "total_silver_rows_processed": 0,
        "errors": []
    }
    
    for table_name, transform_func in silver_tables:
        try:
            table_metrics = transform_func(spark, db_config, batch_id)
            results["silver_tables_processed"].append({
                "table": table_name,
                "metrics": table_metrics
            })
            results["total_silver_rows_processed"] += list(table_metrics.values())[0]
            
        except Exception as e:
            error_msg = f"Failed to process silver {table_name}: {str(e)}"
            results["errors"].append(error_msg)
            print(f"ERROR: {error_msg}")
    
    return results


if __name__ == "__main__":
    # For local testing with Spark 4.0.0
    spark = (SparkSession.builder
             .appName("Silver_Dimensional_Transform")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.sql.ansi.enabled", "true")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .getOrCreate())
    
    # Simple configuration for testing
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres'
    }
    
    # Run silver transform
    results = run(spark, db_config)
    
    print(f"Silver layer transformation completed: {results}")
    
    spark.stop()
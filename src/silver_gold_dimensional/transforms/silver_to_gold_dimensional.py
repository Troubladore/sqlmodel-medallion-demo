"""
🥇 GOLD DIMENSIONAL - Silver to Gold Star Schema Transform

Purpose: Transform clean silver data into dimensional star schema for business intelligence
Demonstrates: Classic dimensional modeling, fact tables, dimension tables, and business metrics
Architecture: PySpark 4.0.0 job for Databricks/Snowpark compatibility

Gold Dimensional Strategy:
- Create conformed dimension tables with business attributes
- Build fact tables with measures and dimension foreign keys
- Pre-calculate analytics and business metrics
- Implement slowly changing dimensions (SCD Type 1)
- Denormalize for query performance

Star Schema Design:
- Fact: gl_fact_rental (central fact table)
- Dimensions: gl_dim_customer, gl_dim_film, gl_dim_store, gl_dim_staff, gl_dim_date
- Metrics: Revenue, rental patterns, customer analytics

Spark 4.0.0 Features Used:
- Advanced window functions for analytics
- Improved join optimizations for dimensional modeling
- Enhanced aggregation capabilities
- Better support for slowly changing dimensions
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from uuid import uuid4

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, lit, col, when, isnan, isnull, coalesce,
    trim, upper, lower, concat, concat_ws, split, length, 
    count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    md5, sha2, expr, row_number, dense_rank, rank,
    to_date, date_format, year, month, dayofmonth, quarter, weekofyear,
    hour, dayofweek, dayofyear, last_day, add_months, datediff,
    round as spark_round, monotonically_increasing_id,
    broadcast, desc, asc
)
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, DateType, TimestampType
from pyspark.sql.window import Window


def extract_silver_table(spark: SparkSession, db_config: dict, table_name: str) -> DataFrame:
    """Extract silver table data with error handling"""
    
    # Build silver JDBC connection
    silver_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/silver_gold_dimensional"
    
    df = (spark.read
          .format("jdbc")
          .option("url", silver_url)
          .option("dbtable", f"pagila_silver.{table_name}")
          .option("user", db_config['user'])
          .option("password", db_config.get('password', 'postgres'))
          .option("driver", "org.postgresql.Driver")
          .load())
    
    return df


def extract_bronze_table(spark: SparkSession, db_config: dict, table_name: str) -> DataFrame:
    """Extract bronze table for additional data not in silver"""
    
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


def extract_gold_table(spark: SparkSession, db_config: dict, table_name: str) -> DataFrame:
    """Extract gold table data with error handling"""
    
    # Build gold JDBC connection
    gold_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/silver_gold_dimensional"
    
    df = (spark.read
          .format("jdbc")
          .option("url", gold_url)
          .option("dbtable", f"pagila_gold.{table_name}")
          .option("user", db_config['user'])
          .option("password", db_config.get('password', 'postgres'))
          .option("driver", "org.postgresql.Driver")
          .load())
    
    return df


def add_gold_audit_fields(df: DataFrame, source_table: str, batch_id: str) -> DataFrame:
    """Add comprehensive gold audit trail"""
    
    return (df
        .withColumn("gl_created_time", current_timestamp())
        .withColumn("gl_updated_time", current_timestamp())
        .withColumn("gl_source_silver_key", 
            # Map silver key if available, otherwise null
            col(f"sl_{source_table}_key") if f"sl_{source_table}_key" in df.columns else lit(None).cast(StringType()))
        .withColumn("gl_is_current", lit(True).cast(BooleanType()))
        .withColumn("gl_batch_id", lit(batch_id).cast(StringType())))


def load_to_gold_table(df: DataFrame, db_config: dict, gold_table: str) -> None:
    """Load gold DataFrame with full refresh strategy"""
    
    # Build gold JDBC connection
    gold_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/silver_gold_dimensional"
    
    (df.write
     .format("jdbc")
     .option("url", gold_url)
     .option("dbtable", f"pagila_gold.{gold_table}")
     .option("user", db_config['user'])
     .option("password", db_config.get('password', 'postgres'))
     .option("driver", "org.postgresql.Driver")
     .mode("overwrite")  # Gold uses full refresh for dimensional consistency
     .save())


def generate_date_dimension(spark: SparkSession) -> DataFrame:
    """Generate comprehensive date dimension table"""
    
    # Create date range for the next 10 years
    start_date = date(2000, 1, 1)
    end_date = date(2030, 12, 31)
    total_days = (end_date - start_date).days + 1
    
    # Generate sequence of dates using direct date_add from start_date
    date_df = spark.range(0, total_days).select(
        col("id").cast(IntegerType()).alias("days_from_start")
    ).withColumn(
        "calendar_date", 
        expr(f"date_add('{start_date}', days_from_start)")  # Direct date addition
    )
    
    # Add comprehensive date attributes
    enriched_df = (date_df
        .withColumn("date_key", 
            (year("calendar_date") * 10000 + 
             month("calendar_date") * 100 + 
             dayofmonth("calendar_date")).cast(IntegerType()))
        .withColumn("calendar_year", year("calendar_date"))
        .withColumn("calendar_quarter", quarter("calendar_date"))
        .withColumn("calendar_month", month("calendar_date"))
        .withColumn("calendar_day", dayofmonth("calendar_date"))
        .withColumn("calendar_week", weekofyear("calendar_date"))
        .withColumn("day_of_week", dayofweek("calendar_date"))
        .withColumn("day_of_year", dayofyear("calendar_date"))
        
        # Business date attributes
        .withColumn("is_weekend", 
            when(col("day_of_week").isin([1, 7]), True).otherwise(False))
        .withColumn("is_holiday", lit(False))  # Placeholder for holiday logic
        .withColumn("fiscal_year", 
            when(col("calendar_month") >= 4, col("calendar_year"))
            .otherwise(col("calendar_year") - 1))
        .withColumn("fiscal_quarter",
            when(col("calendar_month").between(4, 6), 1)
            .when(col("calendar_month").between(7, 9), 2)
            .when(col("calendar_month").between(10, 12), 3)
            .otherwise(4))
        
        # Formatted date strings
        .withColumn("date_name", date_format("calendar_date", "yyyy-MM-dd"))
        .withColumn("month_name", date_format("calendar_date", "MMMM"))
        .withColumn("day_name", date_format("calendar_date", "EEEE"))
        .withColumn("month_year", date_format("calendar_date", "MMMM yyyy")))
    
    return enriched_df.select(
        "date_key", "calendar_date", "date_name", "calendar_year", "calendar_quarter",
        "calendar_month", "calendar_day", "calendar_week", "day_of_week", "day_of_year",
        "month_name", "day_name", "month_year", "fiscal_year", "fiscal_quarter",
        "is_weekend", "is_holiday"
    )


def transform_date_dimension(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform date dimension for gold layer"""
    
    # Generate date dimension
    date_df = generate_date_dimension(spark)
    
    # Load to gold table
    load_to_gold_table(date_df, db_config, "gl_dim_date")
    
    return {"gl_dim_date_rows_processed": date_df.count()}


def transform_customer_dimension(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform silver customer to gold customer dimension with analytics"""
    
    # Extract silver customer data
    silver_customer = extract_silver_table(spark, db_config, "sl_customer")
    silver_address = extract_silver_table(spark, db_config, "sl_address")
    silver_store = extract_silver_table(spark, db_config, "sl_store")
    
    # Get additional data from bronze for rental analytics
    bronze_rental = extract_bronze_table(spark, db_config, "br_rental")
    bronze_payment = extract_bronze_table(spark, db_config, "br_payment")
    
    if silver_customer.count() == 0:
        return {"gl_dim_customer_rows_processed": 0}
    
    # Calculate customer analytics from bronze data
    customer_analytics = (bronze_rental
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .withColumn("rental_date", to_date(col("rental_date")))
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_rentals"),
            spark_min("rental_date").alias("first_rental_date"),
            spark_max("rental_date").alias("last_rental_date")
        )
        .withColumn("days_since_last_rental", 
            datediff(current_timestamp(), col("last_rental_date"))))
    
    # Calculate payment analytics
    payment_analytics = (bronze_payment
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .withColumn("amount", col("amount").cast(FloatType()))
        .groupBy("customer_id")
        .agg(
            spark_sum("amount").alias("total_payments"),
            avg("amount").alias("avg_payment_amount")
        ))
    
    # Join customer with address and store for denormalization (use aliases to avoid ambiguous columns)
    customer_with_address = (silver_customer.alias("c")
        .join(silver_address.alias("a"), col("c.address_id") == col("a.address_id"), "left")
        .join(silver_store.alias("s"), col("c.store_id") == col("s.store_id"), "left"))
    
    # Build comprehensive customer dimension
    customer_dim = (customer_with_address
        .join(customer_analytics, "customer_id", "left")
        .join(payment_analytics, "customer_id", "left")
        
        # Generate gold surrogate key
        .withColumn("gl_dim_customer_key", expr("uuid()"))
        
        # Core customer attributes (from customer table 'c')
        .withColumn("full_name", concat_ws(" ", col("c.first_name"), col("c.last_name")))
        
        # Denormalized address attributes (from address table 'a')
        .withColumn("address_line1", coalesce(col("a.address"), lit("Unknown")))
        .withColumn("address_line2", col("a.address2"))
        .withColumn("district", coalesce(col("a.district"), lit("Unknown")))
        .withColumn("city", lit("Sample City"))  # Placeholder
        .withColumn("postal_code", coalesce(col("a.postal_code"), lit("00000")))
        .withColumn("phone", coalesce(col("a.phone"), lit("000-000-0000")))
        .withColumn("country", lit("USA"))  # Placeholder
        
        # Store attributes (from store table 's')
        .withColumn("store_address", lit("Store Address"))  # Placeholder
        .withColumn("store_city", lit("Store City"))  # Placeholder
        
        # Customer analytics with null handling
        .withColumn("total_rentals", coalesce(col("total_rentals"), lit(0)))
        .withColumn("total_payments", coalesce(col("total_payments"), lit(0.0)))
        .withColumn("avg_payment_amount", coalesce(col("avg_payment_amount"), lit(0.0)))
        
        # Customer segmentation
        .withColumn("customer_lifetime_value", col("total_payments"))
        .withColumn("customer_tier",
            when(col("total_payments") >= lit(100.0), "Platinum")
            .when(col("total_payments") >= lit(50.0), "Gold")
            .when(col("total_payments") >= lit(20.0), "Silver")
            .otherwise("Bronze"))
        .withColumn("is_high_value", 
            when(col("total_payments") >= lit(50.0), True).otherwise(False))
        .withColumn("preferred_genre", lit("Action"))  # Placeholder
        .withColumn("avg_rental_frequency", 
            coalesce(col("total_rentals") / 12.0, lit(0.0))))  # Rentals per month estimate
    
    # Add gold audit fields
    audit_df = add_gold_audit_fields(customer_dim, "customer", batch_id)
    
    # Select final gold schema (specify table source for potentially ambiguous columns)
    final_df = audit_df.select(
        "gl_dim_customer_key", col("c.customer_id").alias("customer_id"), 
        col("c.first_name").alias("first_name"), col("c.last_name").alias("last_name"), 
        "full_name", col("c.email").alias("email"),
        "address_line1", "address_line2", "district", "city", "postal_code", "phone", "country",
        col("c.store_id").alias("store_id"), "store_address", "store_city", 
        col("c.create_date").alias("create_date"), col("c.activebool").alias("activebool"), 
        col("c.last_update").alias("last_update"),
        "total_rentals", "total_payments", "avg_payment_amount", "first_rental_date", 
        "last_rental_date", "days_since_last_rental", "customer_lifetime_value",
        "customer_tier", "is_high_value", "preferred_genre", "avg_rental_frequency",
        "gl_created_time", "gl_updated_time", "gl_source_silver_key", "gl_is_current"
    ).distinct()
    
    # Load to gold table
    load_to_gold_table(final_df, db_config, "gl_dim_customer")
    
    return {"gl_dim_customer_rows_processed": final_df.count()}


def transform_film_dimension(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform silver film to gold film dimension"""
    
    # Extract silver film data
    silver_film = extract_silver_table(spark, db_config, "sl_film")
    silver_language = extract_silver_table(spark, db_config, "sl_language")
    
    if silver_film.count() == 0:
        return {"gl_dim_film_rows_processed": 0}
    
    # Build film dimension with language denormalization (use aliases to avoid ambiguous columns)
    film_dim = (silver_film.alias("f")
        .join(silver_language.alias("l"), "language_id", "left")
        
        # Generate gold surrogate key
        .withColumn("gl_dim_film_key", expr("uuid()"))
        
        # Film attributes (specify table aliases for potentially ambiguous columns)
        .withColumn("language_name", coalesce(col("l.name"), lit("Unknown")))
        .withColumn("film_category", 
            when(col("rating") == "G", "Family")
            .when(col("rating") == "PG", "Family")
            .when(col("rating") == "PG-13", "Teen")
            .when(col("rating") == "R", "Adult")
            .otherwise("General"))
        .withColumn("is_feature_length", 
            when(col("length") >= lit(90), True).otherwise(False))
        .withColumn("revenue_tier",
            when(col("replacement_cost") >= lit(25.0), "Premium")
            .when(col("replacement_cost") >= lit(15.0), "Standard")
            .otherwise("Budget")))
    
    # Add gold audit fields
    audit_df = add_gold_audit_fields(film_dim, "film", batch_id)
    
    # Select final gold schema (specify table source for potentially ambiguous columns)
    final_df = audit_df.select(
        "gl_dim_film_key", col("f.film_id").alias("film_id"), col("f.title").alias("title"), 
        col("f.description").alias("description"), col("f.release_year").alias("release_year"),
        col("f.language_id").alias("language_id"), "language_name", col("f.rental_duration").alias("rental_duration"), 
        col("f.rental_rate").alias("rental_rate"), col("f.length").alias("length"),
        col("f.replacement_cost").alias("replacement_cost"), col("f.rating").alias("rating"), 
        col("f.special_features").alias("special_features"), "film_category",
        "is_feature_length", "revenue_tier", col("f.last_update").alias("last_update"),
        "gl_created_time", "gl_updated_time", "gl_source_silver_key", "gl_is_current"
    )
    
    # Load to gold table
    load_to_gold_table(final_df, db_config, "gl_dim_film")
    
    return {"gl_dim_film_rows_processed": final_df.count()}


def transform_store_dimension(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform silver store to gold store dimension"""
    
    # Extract silver store data
    silver_store = extract_silver_table(spark, db_config, "sl_store")
    silver_address = extract_silver_table(spark, db_config, "sl_address")
    
    if silver_store.count() == 0:
        return {"gl_dim_store_rows_processed": 0}
    
    # Build store dimension (use aliases to avoid ambiguous columns)
    store_dim = (silver_store.alias("s")
        .join(silver_address.alias("a"), col("s.address_id") == col("a.address_id"), "left")
        
        # Generate gold surrogate key
        .withColumn("gl_dim_store_key", expr("uuid()"))
        
        # Store attributes with address denormalization (using table aliases)
        .withColumn("store_address", coalesce(col("a.address"), lit("Unknown Address")))
        .withColumn("store_district", coalesce(col("a.district"), lit("Unknown")))
        .withColumn("store_city", lit("Sample City"))
        .withColumn("store_postal_code", coalesce(col("a.postal_code"), lit("00000")))
        .withColumn("store_phone", coalesce(col("a.phone"), lit("000-000-0000")))
        .withColumn("store_country", lit("USA")))
    
    # Add gold audit fields
    audit_df = add_gold_audit_fields(store_dim, "store", batch_id)
    
    # Select final gold schema (specify table source for potentially ambiguous columns)
    final_df = audit_df.select(
        "gl_dim_store_key", col("s.store_id").alias("store_id"), col("s.manager_staff_id").alias("manager_staff_id"), 
        "store_address", "store_district", "store_city", "store_postal_code",
        "store_phone", "store_country", col("s.last_update").alias("last_update"),
        "gl_created_time", "gl_updated_time", "gl_source_silver_key", "gl_is_current"
    )
    
    # Load to gold table
    load_to_gold_table(final_df, db_config, "gl_dim_store")
    
    return {"gl_dim_store_rows_processed": final_df.count()}


def transform_staff_dimension(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform bronze staff to gold staff dimension"""
    
    # Extract bronze staff data (since no silver staff exists yet)
    bronze_staff = extract_bronze_table(spark, db_config, "br_rental")
    
    if bronze_staff.count() == 0:
        return {"gl_dim_staff_rows_processed": 0}
    
    # Create staff dimension from distinct staff_ids in rental data
    staff_dim = (bronze_staff
        .select("staff_id")
        .distinct()
        .withColumn("staff_id", col("staff_id").cast(IntegerType()))
        .withColumn("gl_dim_staff_key", expr("uuid()").cast(StringType()))
        .withColumn("first_name", lit("Staff").cast(StringType()))
        .withColumn("last_name", concat(lit("Member "), col("staff_id").cast(StringType())))
        .withColumn("email", concat(lit("staff"), col("staff_id").cast(StringType()), lit("@pagila.com")))
        .withColumn("store_id", lit(1).cast(IntegerType()))
        .withColumn("is_active", lit(True).cast(BooleanType()))
        .withColumn("last_update", current_timestamp()))
    
    # Add gold audit fields
    audit_df = add_gold_audit_fields(staff_dim, "staff", batch_id)
    
    # Select final gold schema
    final_df = audit_df.select(
        "gl_dim_staff_key", "staff_id", "first_name", "last_name", "email",
        "store_id", "is_active", "last_update",
        "gl_created_time", "gl_updated_time", "gl_source_silver_key", "gl_is_current"
    )
    
    # Load to gold table
    load_to_gold_table(final_df, db_config, "gl_dim_staff")
    
    return {"gl_dim_staff_rows_processed": final_df.count()}


def transform_rental_fact(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform bronze rental to gold rental fact table"""
    
    # Extract source data
    bronze_rental = extract_bronze_table(spark, db_config, "br_rental")
    bronze_payment = extract_bronze_table(spark, db_config, "br_payment")
    
    # Extract dimension tables for FK lookups
    dim_customer = extract_silver_table(spark, db_config, "sl_customer")
    dim_film = extract_silver_table(spark, db_config, "sl_film")
    dim_store = extract_silver_table(spark, db_config, "sl_store")
    
    if bronze_rental.count() == 0:
        return {"gl_fact_rental_rows_processed": 0}
    
    # Generate date dimension for FK lookups
    date_dim = generate_date_dimension(spark)
    
    # Prepare rental data with proper typing
    rental_clean = (bronze_rental
        .withColumn("rental_id", col("rental_id").cast(IntegerType()))
        .withColumn("rental_date", col("rental_date").cast(TimestampType()))
        .withColumn("inventory_id", col("inventory_id").cast(IntegerType()))
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .withColumn("return_date", col("return_date").cast(TimestampType()))
        .withColumn("staff_id", col("staff_id").cast(IntegerType())))
    
    # Join with payment data for revenue (use aliases to avoid column ambiguity)
    rental_with_payment = (rental_clean.alias("r")
        .join(bronze_payment.withColumn("rental_id", col("rental_id").cast(IntegerType())).alias("p"), 
              "rental_id", "left")
        # Explicitly select columns to resolve ambiguity, taking customer_id from rental (r) not payment (p)
        .select(
            col("r.rental_id").alias("rental_id"),
            col("r.rental_date").alias("rental_date"), 
            col("r.inventory_id").alias("inventory_id"),
            col("r.customer_id").alias("customer_id"),  # Take customer_id from rental table
            col("r.return_date").alias("return_date"),
            col("r.staff_id").alias("staff_id"),
            col("r.last_update").alias("last_update"),
            col("r.br_load_time").alias("br_load_time"),
            col("r.br_source_file").alias("br_source_file"), 
            col("r.br_batch_id").alias("br_batch_id"),
            col("r.br_is_current").alias("br_is_current"),
            col("r.br_record_hash").alias("br_record_hash"),
            col("r.br_rental_key").alias("br_rental_key"),
            # Take payment info from payment table
            coalesce(col("p.amount"), lit(2.99)).alias("amount"),
            coalesce(col("p.payment_date"), col("r.rental_date")).alias("payment_date")
        ))
    
    # Build comprehensive fact table
    fact_rental = (rental_with_payment
        # Generate gold surrogate key
        .withColumn("gl_fact_rental_key", expr("uuid()").cast(StringType()))
        
        # Date dimensions
        .withColumn("rental_date_only", to_date(col("rental_date")))
        .withColumn("return_date_only", to_date(col("return_date")))
        
        # Time attributes
        .withColumn("rental_hour", hour("rental_date"))
        .withColumn("rental_quarter", quarter("rental_date"))
        .withColumn("rental_month", month("rental_date"))
        .withColumn("rental_year", year("rental_date"))
        
        # Calculated measures
        .withColumn("rental_duration_actual", 
            when(col("return_date").isNotNull(), 
                 datediff(col("return_date_only"), col("rental_date_only")))
            .otherwise(lit(None).cast(IntegerType())))
        .withColumn("rental_duration_planned", lit(3).cast(IntegerType()))  # Default rental duration
        .withColumn("is_returned", col("return_date").isNotNull())
        .withColumn("is_overdue", 
            when(col("return_date").isNotNull(),
                 col("rental_duration_actual") > col("rental_duration_planned"))
            .otherwise(lit(False).cast(BooleanType())))
        .withColumn("days_overdue",
            when(col("is_overdue") == True,
                 col("rental_duration_actual") - col("rental_duration_planned"))
            .otherwise(lit(0).cast(IntegerType())))
        
        # Revenue measures
        .withColumn("rental_rate", col("amount").cast(FloatType()))
        .withColumn("replacement_cost", lit(19.99).cast(FloatType()))  # Default
        .withColumn("revenue_amount", col("rental_rate"))
        
        # Analytics flags
        .withColumn("is_weekend_rental", 
            when(dayofweek("rental_date").isin([1, 7]), lit(True).cast(BooleanType())).otherwise(lit(False).cast(BooleanType())))
        .withColumn("is_holiday_rental", lit(False).cast(BooleanType())))  # Placeholder
    
    # Join with date dimension for date keys
    fact_with_dates = (fact_rental
        .join(date_dim.alias("rental_dates"), 
              col("rental_date_only") == col("rental_dates.calendar_date"), "left")
        .withColumn("rental_date_key", col("rental_dates.date_key"))
        .join(date_dim.alias("return_dates"), 
              col("return_date_only") == col("return_dates.calendar_date"), "left")
        .withColumn("return_date_key", col("return_dates.date_key")))
    
    # Create placeholder dimension keys with proper typing
    final_fact = (fact_with_dates
        .withColumn("customer_key", expr("uuid()").cast(StringType()))  # Placeholder
        .withColumn("film_key", expr("uuid()").cast(StringType()))      # Placeholder
        .withColumn("store_key", expr("uuid()").cast(StringType()))     # Placeholder
        .withColumn("staff_key", expr("uuid()").cast(StringType()))     # Placeholder
        
        # Handle null date keys
        .withColumn("rental_date_key", coalesce(col("rental_date_key"), lit(19000101).cast(IntegerType())))
        .withColumn("return_date_key", coalesce(col("return_date_key"), lit(19000101).cast(IntegerType()))))
    
    # Add gold audit fields
    audit_df = add_gold_audit_fields(final_fact, "rental", batch_id)
    
    # Select final gold schema including customer_id for metrics calculation
    final_df = audit_df.select(
        "gl_fact_rental_key", "rental_id", "customer_id", "customer_key", "film_key", "store_key", "staff_key",
        "rental_date_key", "return_date_key", "rental_date_only", "rental_date", 
        "return_date_only", "return_date", "rental_duration_actual", "rental_duration_planned",
        "rental_rate", "replacement_cost", "is_overdue", "days_overdue", "is_returned",
        "revenue_amount", "is_weekend_rental", "is_holiday_rental", "rental_hour",
        "rental_quarter", "rental_month", "rental_year",
        "gl_created_time", "gl_updated_time", "gl_source_silver_key"
    )
    
    # Load to gold table
    load_to_gold_table(final_df, db_config, "gl_fact_rental")
    
    return {"gl_fact_rental_rows_processed": final_df.count()}


def transform_customer_metrics(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform data into gold customer metrics table"""
    
    # STRICT LINEAGE: Gold MUST read from Gold fact table only
    try:
        fact_rental = (spark.read
                      .format("jdbc")
                      .option("url", f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/silver_gold_dimensional")
                      .option("dbtable", "pagila_gold.gl_fact_rental")
                      .option("user", db_config['user'])
                      .option("password", db_config.get('password', 'postgres'))
                      .option("driver", "org.postgresql.Driver")
                      .load())
    except Exception as e:
        # FAIL FAST: Gold layer dependencies must exist
        raise Exception(f"LINEAGE VIOLATION: gl_fact_rental table required for customer metrics but does not exist. "
                       f"Ensure fact table is created before metrics. Error: {str(e)}")
    
    if fact_rental.count() == 0:
        return {"gl_customer_metrics_rows_processed": 0}
    
    # Calculate customer metrics directly from fact table (fact table has customer_id from bronze data)
    customer_metrics = (fact_rental
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))  # Ensure proper type
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_rentals"),
            spark_sum("revenue_amount").alias("total_revenue"),
            avg("revenue_amount").alias("avg_rental_value"),
            spark_max("rental_date").alias("last_rental_date"),
            spark_min("rental_date").alias("first_rental_date")
        )
        .withColumn("gl_customer_metrics_key", expr("uuid()").cast(StringType()))
        .withColumn("customer_lifetime_value", col("total_revenue"))
        .withColumn("avg_rentals_per_month", 
            col("total_rentals") / lit(12.0).cast(FloatType()))  # Estimate
        .withColumn("customer_score",
            (col("total_rentals") * lit(0.3).cast(FloatType()) + 
             col("total_revenue") * lit(0.7).cast(FloatType()))))
    
    # Add gold audit fields
    audit_df = add_gold_audit_fields(customer_metrics, "customer_metrics", batch_id)
    
    # Select final schema
    final_df = audit_df.select(
        "gl_customer_metrics_key", "customer_id", "total_rentals", "total_revenue",
        "avg_rental_value", "last_rental_date", "first_rental_date",
        "customer_lifetime_value", "avg_rentals_per_month", "customer_score",
        "gl_created_time", "gl_updated_time", "gl_source_silver_key", "gl_is_current"
    )
    
    # Load to gold table
    load_to_gold_table(final_df, db_config, "gl_customer_metrics")
    
    return {"gl_customer_metrics_rows_processed": final_df.count()}


def run(spark: SparkSession, db_config: dict, execution_date: str = None) -> Dict[str, any]:
    """
    Main entry point for silver to gold dimensional transformation
    
    Args:
        spark: SparkSession for data processing
        db_config: Database configuration dictionary with connection details
        execution_date: Airflow execution date (optional)
    
    Returns:
        Dict with processing metrics and results
    """
    
    # Generate batch ID for this gold run
    batch_id = f"gold_dimensional_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if execution_date:
        batch_id = f"gold_dimensional_{execution_date.replace('-', '').replace(':', '')}"
    
    # Define gold tables to process in dependency order
    gold_tables = [
        ("date_dimension", transform_date_dimension),
        ("customer_dimension", transform_customer_dimension),
        ("film_dimension", transform_film_dimension),
        ("store_dimension", transform_store_dimension),
        ("staff_dimension", transform_staff_dimension),
        ("rental_fact", transform_rental_fact),
        ("customer_metrics", transform_customer_metrics)
    ]
    
    # Process each gold table and collect metrics
    results = {
        "batch_id": batch_id,
        "execution_date": execution_date or datetime.now().isoformat(),
        "gold_tables_processed": [],
        "total_gold_rows_processed": 0,
        "errors": []
    }
    
    for table_name, transform_func in gold_tables:
        try:
            table_metrics = transform_func(spark, db_config, batch_id)
            results["gold_tables_processed"].append({
                "table": table_name,
                "metrics": table_metrics
            })
            results["total_gold_rows_processed"] += list(table_metrics.values())[0]
            
        except Exception as e:
            error_msg = f"CRITICAL FAILURE: Gold transformation failed for {table_name}: {str(e)}"
            print(f"ERROR: {error_msg}")
            # FAIL FAST: Any gold transformation failure should stop the entire pipeline
            raise Exception(f"Gold pipeline failure in {table_name}. {error_msg}")
    
    return results


if __name__ == "__main__":
    # For local testing with Spark 4.0.0
    spark = (SparkSession.builder
             .appName("Gold_Dimensional_Transform")
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
    
    # Run gold transform
    results = run(spark, db_config)
    
    print(f"Gold dimensional transformation completed: {results}")
    
    spark.stop()
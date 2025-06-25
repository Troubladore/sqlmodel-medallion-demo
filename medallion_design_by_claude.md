  ---
  🥉 BRONZE LAYER (pagila_bronze database)

  Purpose: Raw data landing optimized for heavy batch writes

  Schema Structure:

  - Database: pagila_bronze (separate for performance isolation)
  - Schema: staging_pagila
  - Pattern: All tables prefixed with br_

  Key Features:

  - Lenient Data Types: All source fields as TEXT/VARCHAR to handle data quality issues
  - Audit Fields: br_load_time, br_source_file, br_record_hash, br_is_current, br_batch_id
  - UUID Surrogate Keys: br_[table]_key for data lineage tracking
  - No Constraints: Optimized for fast ingestion

  Tables Created:

  - br_actor, br_film, br_customer, br_rental, br_payment, br_language
  - All source Pagila tables represented with bronze audit pattern

  ---
  🥈 SILVER LAYER (pagila_silver_gold database)

  Purpose: Cleaned, validated data with business rules applied

  Schema Structure:

  - Database: pagila_silver_gold (responsive for analytics)
  - Schema: pagila_silver
  - Pattern: All tables prefixed with sl_

  Key Features:

  - Clean Data Types: Proper integers, timestamps, booleans, decimals
  - Business Rules: Foreign key constraints, data validation
  - Enhanced Metrics: Pre-calculated business KPIs
  - Change Tracking: Silver audit fields for data lineage
  - UUID Surrogate Keys: sl_[table]_key maintaining relationships

  Tables Created:

  - sl_language, sl_customer, sl_film with proper data types
  - Calculated fields like sl_lifetime_value, sl_rental_count, sl_total_revenue
  - Full relationship modeling with TYPE_CHECKING imports

  ---
  🥇 GOLD LAYER (pagila_silver_gold database)

  Purpose: Analytics-optimized marts and fact/dimension tables

  Schema Structure:

  - Database: pagila_silver_gold (same as silver for performance)
  - Schema: pagila_gold
  - Pattern: All tables prefixed with gl_

  Fact Tables:

  - gl_fact_rental: Core rental transaction facts with dimensional keys
    - Time dimensions, duration metrics, revenue calculations
    - Flags for weekend/holiday rentals, overdue analysis

  Dimension Tables:

  - gl_dim_customer: Denormalized customer dimension
    - Address attributes flattened, customer analytics pre-calculated
    - Segmentation: customer tiers, lifetime value, preferred genres
  - gl_dim_film: Comprehensive film dimension
    - Categories/actors denormalized as comma-separated strings
    - Special features broken out as boolean flags
    - Performance metrics and popularity scores
  - gl_dim_date: Complete date dimension
    - All date components, fiscal year, holidays, business flags
    - Relative date indicators for "current" analysis

  Analytics Marts:

  - gl_customer_metrics: Comprehensive customer analytics
    - Rental patterns, payment behavior, lifecycle metrics
    - Risk scores, churn prediction, upsell opportunities
    - Time-series aggregations (monthly, quarterly, yearly)

  ---
  🎯 KEY DESIGN PRINCIPLES IMPLEMENTED:

  1. Performance Separation

  - Bronze: Heavy write database, optimized for ingestion
  - Silver/Gold: Responsive database, optimized for analytics queries

  2. Data Quality Pipeline

  - Bronze → Silver: Data cleansing, type conversion, validation
  - Silver → Gold: Denormalization, aggregation, pre-calculation

  3. Audit & Lineage

  - Bronze: Source tracking (br_source_file, br_batch_id)
  - Silver: Data quality scores (sl_data_quality_score)
  - Gold: Calculation versioning (gl_calculation_version)

  4. Analytics Optimization

  - Denormalized dimensions for query performance
  - Pre-calculated metrics to avoid complex joins
  - Time-based partitioning keys for efficient filtering
  - Star schema design with fact/dimension separation

  5. Business Intelligence Ready

  - Customer segmentation (Bronze/Silver/Gold/Platinum tiers)
  - Behavioral analysis (preferred times, genres, patterns)
  - Predictive scores (churn risk, upsell opportunity)
  - Operational metrics (overdue rates, inventory turnover)

  ---
  🚀 READY FOR ELT PIPELINE DEVELOPMENT

  The medallion architecture is now complete with:
  - ✅ Bronze tables ready for raw data ingestion
  - ✅ Silver tables ready for data transformation/validation
  - ✅ Gold tables ready for analytics and reporting
  - ✅ Database builders configured for both environments
  - ✅ Schema processing orders set correctly
  - ✅ Full relationship modeling with proper foreign keys

  Next Steps: Create PySpark ELT jobs in the transforms/ folders to populate each layer! 🎊
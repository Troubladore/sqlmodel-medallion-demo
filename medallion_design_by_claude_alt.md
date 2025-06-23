  ---
  🏗️ ALTERNATIVE ARCHITECTURE OVERVIEW

  📁 Directory Structure

  src/
  ├── pagila_bronze_alt/          # CDC-focused bronze database
  │   ├── schemas/cdc_bronze.py   # CDC bronze schema
  │   └── tables/bronze/          # 3 CDC tables with jsonb payload
  └── pagila_silver_gold_alt/     # Temporal + simple marts database
      ├── schemas/
      │   ├── temporal_silver.py  # System-versioned dimensions
      │   └── simple_gold.py      # Business marts
      └── tables/
          ├── pagila_silver/      # Temporal tables + current views
          └── pagila_gold/        # 3 simple business marts

  ---
  🥉 BRONZE LAYER ALT: Elegant CDC Pattern

  Design Philosophy: Raw change capture with JSON payload preservation

  Tables Created:
  - br_customer_cdc - Customer change events
  - br_rental_cdc - High-volume rental transactions
  - br_payment_cdc - Payment transaction changes

  Structure (uniform across all CDC tables):
  lsn: int           # WAL Log Sequence Number (ordering guarantee)
  op: str            # Operation: I(nsert), U(pdate), D(elete)
  full_row: JSONB    # Complete row payload (schema evolution friendly)
  load_ts: datetime  # When change was captured

  Key Benefits:
  - ✅ Zero schema evolution issues - JSON handles any structure changes
  - ✅ Transaction ordering - LSN provides perfect sequencing
  - ✅ Complete audit trail - Every I/U/D operation captured
  - ✅ Real-time ready - Designed for streaming CDC from PostgreSQL WAL

  ---
  🥈 SILVER LAYER ALT: PostgreSQL Temporal Magic

  Design Philosophy: System-versioned temporal tables with automatic time-travel

  Temporal Dimensions:
  - dim_customer_hist - Customer dimension with system versioning
  - dim_film_hist - Film dimension with system versioning

  Current-State Views:
  - dim_customer - Current rows only (sys_end = 'infinity')
  - dim_film - Current rows only (sys_end = 'infinity')

  Simple Facts:
  - fact_rental - Rental transactions (partitioned by date)
  - fact_payment - Payment transactions (partitioned by date)

  PostgreSQL Temporal Features:
  -- Automatic SCD Type-2 with system versioning
  CREATE TABLE dim_customer_hist (...) WITH (system_versioning = true);

  -- Time-travel queries (the "wow moment"!)
  SELECT * FROM dim_customer_hist
  FOR SYSTEM_TIME AS OF '2023-07-01'::timestamptz
  WHERE customer_id = 42;

  -- MERGE operations with automatic temporal handling
  MERGE INTO dim_customer_hist tgt USING staging_customer src
  ON tgt.customer_id = src.customer_id
  WHEN MATCHED AND (src.hash <> tgt.hash)
      THEN UPDATE SET ..., sys_end = clock_timestamp()
  WHEN NOT MATCHED THEN INSERT (...);

  ---
  🥇 GOLD LAYER ALT: Simple Business Marts

  Design Philosophy: Focused, demo-friendly business intelligence

  3 Core Marts (exactly as specified in original design):

  1. vw_monthly_revenue (store-month grain)
    - Revenue trends across stores
    - Transaction volume and averages
    - Customer engagement metrics
  2. vw_top_titles (film-month grain)
    - Content performance analysis
    - Rental frequency and revenue
    - Inventory utilization rates
  3. vw_store_scorecard (store-day grain)
    - Daily operational metrics
    - Late return percentages
    - Manager performance tracking

  Business Value:
  - ✅ Executive dashboards ready out-of-the-box
  - ✅ Operational monitoring for store managers
  - ✅ Content optimization for inventory planning
  - ✅ Performance comparison across locations
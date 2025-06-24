# 🥉 Bronze CDC Transform Methodology - Standards & Implementation Guide

> **Purpose**: Document the bronze_cdc transform methodology, standards, and best practices for implementing Change Data Capture (CDC) patterns in the medallion architecture.  
> **Audience**: Data engineers implementing CDC transforms or real-time data ingestion pipelines  
> **Scope**: Incremental CDC ingestion patterns using Spark 4.0.0 + PySpark 4.0.0 with LSN ordering and JSONB payloads

---

## 🎯 Bronze CDC Transform Philosophy

**Bronze CDC** implements the **incremental CDC ingestion** pattern optimized for:
- Real-time or near real-time analytics
- Change tracking and audit trails
- Incremental processing efficiency
- LSN-based ordering guarantees
- JSONB payload flexibility

**Key Principles**:
1. **LSN Ordering**: Log Sequence Numbers provide strict ordering guarantees
2. **Incremental Append**: Only process new changes since last watermark
3. **JSONB Payloads**: Flexible schema evolution with queryable JSON
4. **Watermark Management**: Track processing progress with LSN checkpoints
5. **Operation Tracking**: Capture Insert/Update/Delete operations

---

## 🏗️ CDC Transform Architecture Patterns

### **1. CDC Data Model Pattern**

```python
# CDC table structure (differs from bronze_basic)
class BrCustomerCdc(SQLModel, table=True):
    __tablename__ = "br_customer_cdc"
    __table_args__ = {"schema": "cdc_bronze"}

    # CDC-specific fields
    lsn: int = Field(sa_column=Column(BIGINT, primary_key=True))  # Ordering guarantee
    op: str = Field(sa_column=Column(CHAR(1), nullable=False))   # I/U/D operations
    full_row: Dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))  # Complete payload
    load_ts: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False))  # Watermark
```

**CDC vs Bronze Basic Differences**:
| Aspect | Bronze Basic | Bronze CDC |
|--------|-------------|------------|
| **Primary Key** | UUID surrogate | LSN sequence |
| **Data Format** | Structured columns (TEXT) | JSONB payloads |
| **Load Strategy** | Full refresh (overwrite) | Incremental (append) |
| **Ordering** | Batch timestamps | LSN sequence |
| **Change Tracking** | Snapshot state | Operation history |

### **2. CDC Function Decomposition Pattern**

```python
# Core CDC functions (modular and testable)
def extract_cdc_changes(spark: SparkSession, db_config: dict, table_name: str, 
                       last_lsn: int = 0, watermark_minutes: int = 5) -> DataFrame:
    """Extract incremental changes based on watermarks"""

def create_cdc_payload(df: DataFrame, table_name: str, batch_id: str) -> DataFrame:
    """Convert source data to CDC format with JSONB payload and LSN"""

def get_last_lsn(spark: SparkSession, db_config: dict, cdc_table: str) -> int:
    """Get highest LSN for watermark-based incremental processing"""

def load_to_cdc_table(df: DataFrame, db_config: dict, cdc_table: str) -> None:
    """Load with incremental append strategy"""

def transform_[table]_cdc_to_bronze(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Individual CDC table transform with watermark management"""
```

### **3. Watermark-Based Extraction Pattern**

```python
def extract_cdc_changes(spark: SparkSession, db_config: dict, table_name: str, 
                       last_lsn: int = 0, watermark_minutes: int = 5) -> DataFrame:
    """Extract CDC changes using timestamp-based simulation"""
    
    source_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/pagila"
    
    # Extract with time-based watermark (simulates CDC)
    watermark_time = datetime.now() - timedelta(minutes=watermark_minutes)
    
    df = (spark.read
          .format("jdbc")
          .option("url", source_url)
          .option("dbtable", f"public.{table_name}")
          .option("user", db_config['user'])
          .option("password", db_config.get('password'))
          .option("driver", "org.postgresql.Driver")
          .load())
    
    # Filter for changes (in production: from WAL/CDC stream)
    if 'last_update' in df.columns:
        df = df.filter(col('last_update') >= lit(watermark_time))
    
    # Cast all columns to string for JSON payload
    for column in df.columns:
        df = df.withColumn(column, col(column).cast(StringType()))
    
    return df
```

**Watermark Strategy**:
- Extract only changes since last processed LSN
- Use time-based filtering for CDC simulation
- In production: extract from WAL or CDC streams
- Handle empty extractions gracefully

### **4. JSONB Payload Creation Pattern**

```python
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
        .withColumn("op", lit("I"))  # Operation type (I/U/D)
        .withColumn("load_ts", current_timestamp())
        .select("lsn", "op", "full_row", "load_ts"))
    
    return df_cdc
```

**JSONB Benefits**:
- Schema evolution without table changes
- Query flexibility with JSON operators
- Preserve complete source record
- Handle nested/complex data structures

### **5. LSN Management Pattern**

```python
def get_last_lsn(spark: SparkSession, db_config: dict, cdc_table: str) -> int:
    """Get highest LSN for incremental processing"""
    
    try:
        target_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/bronze_cdc"
        
        df = (spark.read
              .format("jdbc")
              .option("url", target_url)
              .option("dbtable", f"cdc_bronze.{cdc_table}")
              .option("user", db_config['user'])
              .option("password", db_config.get('password'))
              .option("driver", "org.postgresql.Driver")
              .load())
        
        if df.count() == 0:
            return 0
            
        return df.agg(spark_max("lsn")).collect()[0][0] or 0
        
    except Exception:
        return 0  # Table empty or doesn't exist

# Adjust LSNs for continuous sequence
if last_lsn > 0:
    cdc_df = cdc_df.withColumn("lsn", col("lsn") + lit(last_lsn))
```

**LSN Management**:
- Monotonic increasing sequence numbers
- No gaps or duplicates allowed
- Continue sequence from last processed LSN
- Handle first-time processing (LSN=0)

---

## 📋 CDC Implementation Standards

### **1. Incremental Processing Standards**

**Batch ID Generation**:
```python
# CDC batch format: bronze_cdc_YYYYMMDD_HHMMSS
batch_id = f"bronze_cdc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
if execution_date:
    batch_id = f"bronze_cdc_{execution_date.replace('-', '').replace(':', '')}"
```

**CDC Table Processing**:
```python
# Define CDC tables (subset of bronze_basic tables)
cdc_tables = [
    ("customer", transform_customer_cdc_to_bronze),
    ("payment", transform_payment_cdc_to_bronze),
    ("rental", transform_rental_cdc_to_bronze)
]

# Process with watermark management
for table_name, transform_func in cdc_tables:
    try:
        table_metrics = transform_func(spark, db_config, batch_id)
        results["cdc_tables_processed"].append({
            "table": table_name,
            "metrics": table_metrics
        })
    except Exception as e:
        # Continue processing but track errors
        results["errors"].append(f"CDC failed for {table_name}: {str(e)}")
```

### **2. CDC Load Strategy**

```python
def load_to_cdc_table(df: DataFrame, db_config: dict, cdc_table: str) -> None:
    """Load CDC DataFrame with incremental append strategy"""
    
    target_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/bronze_cdc"
    
    (df.write
     .format("jdbc")
     .option("url", target_url)
     .option("dbtable", f"cdc_bronze.{cdc_table}")
     .option("user", db_config['user'])
     .option("password", db_config.get('password'))
     .option("driver", "org.postgresql.Driver")
     .option("createTableColumnTypes", "lsn BIGINT, op CHAR(1), full_row JSONB, load_ts TIMESTAMPTZ")
     .option("stringtype", "unspecified")  # Let PostgreSQL handle type inference
     .mode("append")  # CDC always appends, never overwrites
     .save())
```

**Append Strategy Benefits**:
- Preserve complete change history
- Enable point-in-time recovery
- Support change analytics
- Prevent data loss from failed runs

### **3. CDC Validation Standards**

```python
# CDC-specific validations
def validate_cdc_data(conn, table: str) -> dict:
    # LSN sequence validation (critical for CDC)
    lsn_check = conn.execute(text(f"""
        SELECT 
            COUNT(*) as total_lsns,
            COUNT(DISTINCT lsn) as unique_lsns,
            MAX(lsn) as max_lsn,
            MIN(lsn) as min_lsn
        FROM {table}
    """))
    
    # JSONB payload validation
    payload_check = conn.execute(text(f"""
        SELECT COUNT(*) 
        FROM {table} 
        WHERE full_row IS NOT NULL 
        AND jsonb_typeof(full_row) = 'object'
    """))
    
    # Operation type distribution
    op_check = conn.execute(text(f"""
        SELECT op, COUNT(*) 
        FROM {table} 
        GROUP BY op
    """))
    
    return {
        "lsn_unique": total_lsns == unique_lsns,
        "payload_valid": valid_payloads == record_count,
        "operations": dict(op_check.fetchall())
    }
```

**CDC Quality Checks**:
- LSN uniqueness and continuity
- JSONB payload validity
- Operation type distribution
- Watermark progression

---

## 📊 CDC Monitoring & Metrics

### **Real-Time Lag Monitoring**:
```python
def check_cdc_lag(target_engine, table: str) -> dict:
    """Monitor CDC processing lag"""
    
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
    
    result = conn.execute(lag_query).fetchone()
    
    return {
        "lag_minutes": float(result[0]),
        "total_records": int(result[1]),
        "latest_lsn": int(result[2]),
        "alert_required": float(result[0]) > 10  # Alert if >10min lag
    }
```

### **CDC Results Structure**:
```python
results = {
    "batch_id": "bronze_cdc_20250624_010030",
    "execution_date": "2025-06-24T01:00:30",
    "cdc_tables_processed": [
        {"table": "customer", "metrics": {"customer_cdc_rows_processed": 599}},
        {"table": "payment", "metrics": {"payment_cdc_rows_processed": 16049}},
        {"table": "rental", "metrics": {"rental_cdc_rows_processed": 16044}}
    ],
    "total_cdc_rows_processed": 32692,
    "errors": []
}
```

### **CDC Success Metrics**:
- **LSN continuity**: No gaps in LSN sequence
- **Payload integrity**: 100% valid JSONB payloads
- **Lag monitoring**: Processing lag < 10 minutes
- **Incremental accuracy**: Correct change capture

---

## 🕒 CDC Scheduling Standards

### **Micro-Batch Configuration**:
```python
# CDC DAG configuration
DAG_ID = 'bronze_cdc_ingestion'
SCHEDULE_INTERVAL = '*/5 * * * *'  # Every 5 minutes (vs daily for bronze_basic)
MAX_ACTIVE_RUNS = 1  # Prevent overlapping CDC runs
DEFAULT_ARGS = {
    'retries': 2,  # More retries for CDC reliability
    'retry_delay': timedelta(minutes=2)  # Shorter delay for real-time
}
```

### **CDC Task Groups**:
```python
with TaskGroup('preflight_checks') as preflight_group:
    check_source = PythonOperator(...)      # Source connectivity
    check_target = PythonOperator(...)      # Target connectivity  
    check_lag = PythonOperator(...)         # CDC lag monitoring (NEW)
```

**CDC-Specific Tasks**:
- **CDC lag monitoring**: Alert on high processing lag
- **Watermark validation**: Ensure LSN progression
- **Real-time alerting**: Immediate notification on failures

---

## 🚨 CDC Common Issues & Solutions

### **Issue: LSN Gaps or Duplicates**
**Root Cause**: Concurrent processing or failed LSN management  
**Solution**: Use window functions for monotonic LSN generation  
**Prevention**: Single active run, proper watermark handling

### **Issue: JSONB Serialization Errors**
**Root Cause**: Complex data types or encoding issues  
**Solution**: Cast all columns to string before JSON conversion  
**Prevention**: Validate source data types

### **Issue: CDC Lag Accumulation**
**Root Cause**: Processing slower than data arrival rate  
**Solution**: Optimize Spark configuration, parallel processing  
**Prevention**: Monitor lag metrics, scale resources

### **Issue: Watermark State Loss**
**Root Cause**: Failed job recovery or state corruption  
**Solution**: Checkpoint LSN state, replay from last known good LSN  
**Prevention**: Robust error handling, state persistence

---

## 📚 CDC Usage Examples

### **Basic CDC Transform**:
```python
def transform_payment_cdc_to_bronze(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform payment CDC with full methodology"""
    
    # Get watermark for incremental processing
    last_lsn = get_last_lsn(spark, db_config, "br_payment_cdc")
    
    # Extract CDC changes
    source_df = extract_cdc_changes(spark, db_config, "payment", last_lsn)
    
    if source_df.count() == 0:
        return {"payment_cdc_rows_processed": 0}
    
    # Create CDC payload with LSN
    cdc_df = create_cdc_payload(source_df, "payment", batch_id)
    
    # Adjust LSNs for continuity
    if last_lsn > 0:
        cdc_df = cdc_df.withColumn("lsn", col("lsn") + lit(last_lsn))
    
    # Load incrementally
    load_to_cdc_table(cdc_df, db_config, "br_payment_cdc")
    
    return {"payment_cdc_rows_processed": cdc_df.count()}
```

### **CDC Query Examples**:
```sql
-- Query JSONB payloads
SELECT 
    lsn,
    op,
    full_row->>'payment_id' as payment_id,
    full_row->>'amount' as amount,
    load_ts
FROM cdc_bronze.br_payment_cdc 
WHERE full_row->>'customer_id' = '123'
ORDER BY lsn DESC;

-- Monitor CDC lag
SELECT 
    'br_payment_cdc' as table,
    COUNT(*) as total_changes,
    MAX(lsn) as latest_lsn,
    EXTRACT(EPOCH FROM (now() - MAX(load_ts)))/60 as lag_minutes
FROM cdc_bronze.br_payment_cdc;
```

---

## 🔄 CDC Adaptation Guidelines

### **When to Use Bronze CDC**:
- Real-time analytics requirements
- Change tracking and auditing needed
- Large datasets with small change rates
- Stream processing capabilities available
- Event-driven architectures

### **When to Consider Alternatives**:
- Simple daily batch requirements → **bronze_basic**
- Historical snapshots needed → **bronze_basic**
- Limited real-time infrastructure → **bronze_basic**

### **Production CDC Upgrade Path**:
```
Phase 1: Simulated CDC (timestamp-based) → Current Implementation
Phase 2: Database CDC (WAL/logical replication)
Phase 3: Streaming CDC (Kafka/Kinesis)
Phase 4: Delta Live Tables (Databricks) or Snowpipe (Snowflake)
```

---

## 🎯 CDC Success Criteria

**Implementation Complete When**:
- [ ] CDC tables process incrementally without gaps
- [ ] LSN sequences are continuous and unique
- [ ] JSONB payloads are valid and queryable
- [ ] Processing lag remains under SLA thresholds
- [ ] Watermark management handles failures gracefully
- [ ] Real-time monitoring and alerting active

**Performance Benchmarks**:
- 32,692+ CDC rows processed in <2 minutes
- LSN sequence 1-16049 with no gaps
- <5 minute processing lag for real-time analytics
- 100% JSONB payload integrity

---

**🚀 Next Steps**: Use this bronze_cdc methodology for real-time analytics, then upgrade to true database CDC (WAL replication) and streaming platforms (Kafka, Delta Live Tables) for production-scale implementations.
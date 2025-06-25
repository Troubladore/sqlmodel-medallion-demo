# 🥉 Bronze Basic Transform Methodology - Standards & Implementation Guide

> **Purpose**: Document the bronze_basic transform methodology, standards, and best practices for implementing full-refresh batch ingestion transforms in the medallion architecture.  
> **Audience**: Data engineers implementing bronze_basic transforms or adapting to other bronze methodologies  
> **Scope**: Full-refresh, batch-based data ingestion patterns using Spark 4.0.0 + PySpark 4.0.0

---

## 🎯 Bronze Basic Transform Philosophy

**Bronze Basic** implements the **full-refresh, batch ingestion** pattern optimized for:
- Daily analytical workloads
- Complete historical accuracy 
- Lenient data quality handling
- Comprehensive audit trails
- Schema evolution resilience

**Key Principles**:
1. **Lenient Typing**: All source data cast to TEXT in bronze layer
2. **Full Refresh**: Complete table reload each batch (overwrite mode)
3. **Comprehensive Auditing**: Complete lineage and processing metadata
4. **Error Resilience**: Continue processing other tables if one fails
5. **Schema Evolution**: Handle source schema changes gracefully

---

## 🏗️ Transform Architecture Patterns

### **1. Function Decomposition Pattern**

```python
# Core transform functions (modular and testable)
def extract_source_table(spark: SparkSession, db_config: dict, table_name: str) -> DataFrame:
    """Extract single source table with lenient schema"""

def add_bronze_audit_fields(df: DataFrame, source_table: str, batch_id: str) -> DataFrame:  
    """Add comprehensive bronze audit trail"""

def load_to_bronze_table(df: DataFrame, db_config: dict, bronze_table: str) -> None:
    """Load with full refresh strategy (overwrite mode)"""

def transform_[table]_to_bronze(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Individual table transform with metrics and error handling"""

def run(spark: SparkSession, db_config: dict, execution_date: str = None) -> Dict[str, any]:
    """Main orchestration function with comprehensive error handling"""
```

### **2. Lenient Data Extraction Pattern**

```python
def extract_source_table(spark: SparkSession, db_config: dict, table_name: str) -> DataFrame:
    """Extract with lenient typing for data quality resilience"""
    
    # Build JDBC connection
    source_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/pagila"
    
    # Extract with lenient approach
    df = (spark.read
          .format("jdbc")
          .option("url", source_url)
          .option("dbtable", f"public.{table_name}")
          .option("user", db_config['user'])
          .option("password", db_config.get('password'))
          .option("driver", "org.postgresql.Driver")
          .load())
    
    # Cast ALL columns to string for bronze lenient typing
    for column in df.columns:
        df = df.withColumn(column, col(column).cast(StringType()))
    
    return df
```

**Lenient Typing Strategy**:
- Cast ALL source columns to `StringType()` in bronze layer
- Preserve original values for debugging and data lineage
- Handle bad data gracefully (nulls, malformed dates, etc.)
- Type conversion happens in silver layer with proper validation

### **3. Comprehensive Audit Trail Pattern**

```python
def add_bronze_audit_fields(df: DataFrame, source_table: str, batch_id: str) -> DataFrame:
    """Add comprehensive bronze audit trail with UUID keys"""
    
    return (df
        # Bronze surrogate key (UUID for business meaning)
        .withColumn("br_[table]_key", expr("uuid()"))
        
        # Processing metadata
        .withColumn("br_load_time", current_timestamp())
        .withColumn("br_source_file", lit(f"pagila.public.{source_table}"))
        .withColumn("br_batch_id", lit(batch_id))
        .withColumn("br_is_current", lit(True))
        
        # Data lineage hash
        .withColumn("br_record_hash", 
            sha2(concat_ws("|", *[col(c) for c in df.columns]), 256)))
```

**Required Bronze Audit Fields**:
- `br_[table]_key`: UUID surrogate key for bronze record identity
- `br_load_time`: Timestamp when record was loaded into bronze
- `br_source_file`: Source system/file identifier for lineage
- `br_batch_id`: Batch/job identifier for processing tracking
- `br_is_current`: Record currency flag for soft deletes
- `br_record_hash`: SHA-256 hash of source columns for change detection

### **4. Full Refresh Load Pattern**

```python
def load_to_bronze_table(df: DataFrame, db_config: dict, bronze_table: str) -> None:
    """Load with full refresh strategy for daily batch processing"""
    
    target_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/bronze_basic"
    
    (df.write
     .format("jdbc")
     .option("url", target_url)
     .option("dbtable", f"staging_pagila.{bronze_table}")
     .option("user", db_config['user'])
     .option("password", db_config.get('password'))
     .option("driver", "org.postgresql.Driver")
     .mode("overwrite")  # Full refresh - complete table replacement
     .save())
```

**Full Refresh Benefits**:
- **Complete accuracy**: Every run represents full source state
- **Simple recovery**: Re-run any date to fix data issues
- **Schema evolution**: Handle column additions/removals automatically
- **No watermark complexity**: No need to track incremental state

---

## 📋 Implementation Standards

### **1. Batch Processing Standards**

**Batch ID Generation**:
```python
# Standard format: bronze_basic_YYYYMMDD
batch_id = f"bronze_basic_{datetime.now().strftime('%Y%m%d')}"
if execution_date:
    batch_id = f"bronze_basic_{execution_date.replace('-', '')}"
```

**Table Processing Loop**:
```python
# Define all tables to process
source_tables = [
    ("actor", transform_actor_to_bronze),
    ("customer", transform_customer_to_bronze), 
    ("film", transform_film_to_bronze),
    ("language", transform_language_to_bronze),
    ("payment", transform_payment_to_bronze),
    ("rental", transform_rental_to_bronze)
]

# Process each table with error isolation
for table_name, transform_func in source_tables:
    try:
        table_metrics = transform_func(spark, db_config, batch_id)
        results["tables_processed"].append({
            "table": table_name,
            "metrics": table_metrics
        })
    except Exception as e:
        error_msg = f"Failed to process {table_name}: {str(e)}"
        results["errors"].append(error_msg)
        # Continue processing other tables
```

### **2. Error Handling Standards**

**Error Isolation Pattern**:
- Process each table independently
- Capture errors without stopping entire job
- Continue processing remaining tables
- Return comprehensive error reporting

**Validation Standards**:
```python
# Record count validation
source_count = source_df.count()
bronze_count = bronze_df.count()
assert source_count == bronze_count, f"Count mismatch: {source_count} vs {bronze_count}"

# Audit field validation  
audit_count = bronze_df.filter(col("br_load_time").isNotNull()).count()
assert audit_count == bronze_count, "Missing audit fields"

# Batch consistency validation
batch_check = bronze_df.select("br_batch_id").distinct().count()
assert batch_check == 1, "Multiple batch IDs in single load"
```

### **3. Performance Standards**

**Spark 4.0.0 Configuration**:
```python
spark = (SparkSession.builder
         .appName(f"Bronze_Basic_Pagila_{execution_date}")
         # Enhanced Adaptive Query Execution (AQE)
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true")
         # ANSI SQL mode for data integrity
         .config("spark.sql.ansi.enabled", "true")
         # Performance optimizations
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .config("spark.jars", "/opt/spark/jars/postgresql.jar")
         .getOrCreate())
```

**JDBC Performance Settings**:
- Use official PostgreSQL JDBC driver
- Configure driver in both Spark classpath and session config
- Use connection pooling for multiple table loads

---

## 📊 Metrics & Monitoring Standards

### **Results Structure**:
```python
results = {
    "batch_id": "bronze_basic_20250624",
    "execution_date": "2025-06-24",
    "tables_processed": [
        {"table": "actor", "metrics": {"rows_processed": 200}},
        {"table": "customer", "metrics": {"rows_processed": 599}},
        # ... more tables
    ],
    "total_rows_processed": 33898,
    "errors": []  # List of error messages if any
}
```

### **Success Metrics**:
- **All tables processed**: No errors in `results["errors"]`
- **Expected row counts**: Match source table counts
- **Audit fields populated**: 100% of records have audit metadata
- **Batch consistency**: Single batch_id across all records

### **Monitoring Queries**:
```sql
-- Validate latest batch
SELECT 
    br_batch_id,
    COUNT(*) as total_records,
    MIN(br_load_time) as batch_start,
    MAX(br_load_time) as batch_end
FROM staging_pagila.br_actor 
GROUP BY br_batch_id 
ORDER BY batch_start DESC 
LIMIT 5;

-- Check data freshness
SELECT 
    table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    (SELECT COUNT(*) FROM staging_pagila.br_actor WHERE br_batch_id = 
     (SELECT MAX(br_batch_id) FROM staging_pagila.br_actor)) as latest_count
FROM pg_tables 
WHERE schemaname = 'staging_pagila';
```

---

## 🔄 Airflow DAG Integration Standards

### **DAG Configuration**:
```python
DAG_ID = 'bronze_basic_pagila_ingestion'
SCHEDULE_INTERVAL = '@daily'  # Full refresh daily
MAX_ACTIVE_RUNS = 1  # Prevent overlapping runs
```

### **Task Structure**:
```python
# Standard task flow for bronze_basic
start_task >> preflight_checks >> run_transform >> validate_data >> notify_completion >> end_task

# Preflight checks (parallel)
with TaskGroup('preflight_checks') as preflight_group:
    check_source = PythonOperator(...)  # Source connectivity
    check_target = PythonOperator(...)  # Target connectivity
```

### **Validation Integration**:
- Validate record counts match source
- Check audit field population
- Verify batch consistency
- Alert on processing errors

---

## 🚨 Common Issues & Solutions

### **Issue: Type Conversion Errors**
**Root Cause**: Source data has incompatible types  
**Solution**: Cast ALL columns to StringType in bronze layer  
**Prevention**: Use lenient typing pattern consistently

### **Issue: Memory Issues with Large Tables**
**Root Cause**: Loading large tables without partitioning  
**Solution**: Enable Spark AQE and coalescing  
**Prevention**: Use Spark 4.0.0 adaptive features

### **Issue: JDBC Connection Failures**
**Root Cause**: Database connectivity or driver issues  
**Solution**: Test in preflight checks, proper JDBC config  
**Prevention**: Use connection validation patterns

### **Issue: Audit Field Inconsistencies**
**Root Cause**: Different batch IDs or missing timestamps  
**Solution**: Centralized audit field generation  
**Prevention**: Use standardized audit function

---

## 📚 Usage Examples

### **Basic Transform Implementation**:
```python
def transform_actor_to_bronze(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Transform actor table to bronze with full methodology"""
    
    # Extract source data
    source_df = extract_source_table(spark, db_config, "actor")
    
    # Add bronze audit fields
    bronze_df = add_bronze_audit_fields(source_df, "actor", batch_id)
    
    # Load to bronze table
    load_to_bronze_table(bronze_df, db_config, "br_actor")
    
    return {"rows_processed": bronze_df.count()}
```

### **Running Bronze Basic Transform**:
```python
# Initialize Spark with standard configuration
spark = create_spark_session("Bronze_Basic_Transform")

# Database configuration
db_config = {
    'host': 'host.docker.internal',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres'
}

# Run transform
results = run(spark, db_config, execution_date="2025-06-24")
print(f"Processed {results['total_rows_processed']} rows")
```

---

## 🔄 Adaptation Guidelines

### **When to Use Bronze Basic**:
- Daily or less frequent data loads
- Complete historical accuracy required
- Source systems support full extraction
- Schema evolution is common
- Data quality issues need lenient handling

### **When to Consider Alternatives**:
- Real-time or streaming requirements → **bronze_cdc**
- Large datasets with minimal changes → **bronze_incremental**
- Event-driven architectures → **bronze_streaming**

### **Adapting to Other Bronze Patterns**:
```bash
# Copy bronze_basic structure
cp -r src/bronze_basic/transforms/ src/bronze_[new_pattern]/transforms/

# Modify key components:
# 1. Change load strategy (overwrite → append for incremental)
# 2. Update table schemas (audit fields → CDC fields)  
# 3. Adapt extraction logic (full → incremental)
# 4. Modify validation rules (batch → watermark)
```

---

## 🎯 Success Criteria

**Implementation Complete When**:
- [ ] All source tables processed without errors
- [ ] Record counts match between source and bronze
- [ ] Audit fields populated on 100% of records
- [ ] Batch processing completes within SLA
- [ ] Error handling tested and validated
- [ ] Monitoring and alerting configured

**Performance Benchmarks**:
- 33,898+ rows processed in <5 minutes
- 0 processing errors
- Complete audit trail
- Full refresh reliability

---

---

## 🧬 Evolution Tracking DNA (CRITICAL)

**Every bronze_basic transform development session MUST update evolution documentation.**

📚 **CENTRALIZED STANDARDS**: All evolution tracking requirements are defined in:

👉 **[`docs/EVOLUTION_TRACKING_STANDARDS.md`](../../../docs/EVOLUTION_TRACKING_STANDARDS.md)** 👈

### **Bronze Basic Transform Specific Requirements**:

**Transform Changelog**: `src/bronze_basic/transforms/CHANGELOG.md` - REQUIRED after ANY changes

**Key Transform Metrics to Track**:
- Processing rate (rows/second per table transformation)
- Error isolation success rate (continuing after individual table failures)
- Audit completeness (100% audit field population)
- JDBC performance efficiency and throughput
- Memory stability patterns across table processing

**Focus Areas for Transform Evolution**:
- Processing methodology (full-refresh patterns, error isolation, audit standards)
- Performance optimization (JDBC configs, Spark settings, memory management)
- Error resilience (isolation patterns, recovery, validation frameworks)
- Standards maturation (coding patterns, configuration management)
- Replication guidelines (template updates for other bronze patterns)

**📖 For complete templates, automation, and standards, see [`docs/EVOLUTION_TRACKING_STANDARDS.md`](../../../docs/EVOLUTION_TRACKING_STANDARDS.md)**

---

**🚀 Next Steps**: Use this bronze_basic methodology as the foundation for other bronze patterns (CDC, streaming, incremental), adapting the specific processing logic while preserving the proven infrastructure and error handling patterns. **CRITICAL**: Document all adaptations and improvements in the evolution tracking system to build institutional knowledge.
# 🥉 Bronze Basic Layer - Lessons Learned & Best Practices

> **Purpose**: Document all lessons learned, best practices, and architectural patterns from implementing the bronze_basic layer with cutting-edge Spark 4.0.0 + PySpark 4.0.0.  
> **Audience**: Data engineers implementing bronze_cdc and other medallion layers  
> **Scope**: Production-ready patterns for medallion lakehouse architecture

---

## 🎯 Executive Summary

This bronze_basic implementation successfully demonstrates:
- **Cutting-edge technology**: Spark 4.0.0 + PySpark 4.0.0 with latest features
- **Production architecture**: No shortcuts, enterprise-grade patterns
- **Cloud portability**: Ready for Databricks Delta Live Tables and Snowflake Snowpark
- **Complete medallion compliance**: Proper audit trails, data quality, error handling

**Final Results**: 33,898 rows successfully processed across 6 tables with full audit trail.

---

## 🔧 Technology Stack & Versions

### ✅ **Cutting-Edge Stack (Proven Working)**
```yaml
Core Technologies:
  - Apache Spark: 4.0.0 (latest major release)
  - PySpark: 4.0.0 (matching version)
  - Java: OpenJDK 17 (required for Spark 4.0.0)
  - Python: 3.10
  - Apache Airflow: 2.9.1

Database & Connectivity:
  - PostgreSQL: 17.5 (latest)
  - PostgreSQL JDBC Driver: 42.7.3
  - SQLAlchemy: 1.4.0+
  - Pandas: 1.3.0+

Container Environment:
  - Docker: Latest
  - Base Image: apache/airflow:2.9.1-python3.10
```

### 🚨 **Critical Version Dependencies**
- **Spark 4.0.0 REQUIRES Java 17+** (not Java 8/11)
- **PySpark 4.0.0 REQUIRES Spark 4.0.0** (not 3.x)
- **JDBC driver must be in `/opt/spark/jars/`** for proper loading

---

## 🏗️ Architecture Patterns

### **1. Docker Environment Setup**

**Dockerfile Pattern** (`docker/airflow/Dockerfile`):
```dockerfile
FROM apache/airflow:2.9.1-python3.10

USER root

# Install Java 17, Spark 4.0.0, and PostgreSQL client
RUN apt-get update && \
    apt-get install -y default-jdk wget curl unzip postgresql-client && \
    wget https://archive.apache.org/dist/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz && \
    tar -xzf spark-4.0.0-bin-hadoop3.tgz -C /opt/ && \
    ln -s /opt/spark-4.0.0-bin-hadoop3 /opt/spark && \
    rm spark-4.0.0-bin-hadoop3.tgz

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=/home/airflow/.local/bin:$PATH:$SPARK_HOME/bin

# Add PostgreSQL JDBC driver to Spark
RUN mkdir -p /opt/jdbc && \
    wget -O /opt/jdbc/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    cp /opt/jdbc/postgresql.jar /opt/spark/jars/

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
```

**Requirements Pattern** (`docker/airflow/requirements.txt`):
```text
apache-airflow==2.9.1
apache-airflow-providers-apache-spark
pyspark==4.0.0
pandas>=1.3.0
sqlalchemy>=1.4.0
```

### **2. Spark 4.0.0 Configuration**

**Enhanced Configuration for Cutting-Edge Features**:
```python
spark = (SparkSession.builder
         .appName(f"Bronze_Basic_Pagila_{execution_date}")
         # Enhanced Adaptive Query Execution (AQE) in Spark 4.0.0
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true")
         # ANSI SQL mode for better data integrity (default in 4.0.0)
         .config("spark.sql.ansi.enabled", "true")
         # Enhanced performance settings
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         # JDBC driver configuration
         .config("spark.jars", "/opt/spark/jars/postgresql.jar")
         .getOrCreate())
```

### **3. Bronze Layer Data Model**

**Bronze Table Pattern** (Lenient Typing + Comprehensive Audit):
```python
class BrActor(SQLModel, table=True):
    __tablename__ = "br_actor"
    __table_args__ = {"schema": "staging_pagila"}

    # Bronze surrogate key
    br_actor_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Source data fields (lenient types for data quality issues)
    actor_id: Optional[str] = Field(default=None, nullable=True)  # TEXT to handle bad data
    first_name: Optional[str] = Field(default=None, nullable=True)
    last_name: Optional[str] = Field(default=None, nullable=True)
    last_update: Optional[str] = Field(default=None, nullable=True)  # TEXT to handle bad timestamps
    
    # Bronze audit fields (REQUIRED for all bronze tables)
    br_load_time: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False,
                        default=text("now()"), server_default=text("now()"))
    )
    br_source_file: Optional[str] = Field(default=None, nullable=True, max_length=500)
    br_record_hash: Optional[str] = Field(default=None, nullable=True, max_length=64)
    br_is_current: bool = Field(default=True, nullable=False)
    br_batch_id: Optional[str] = Field(default=None, nullable=True, max_length=100)
```

### **4. Transform Job Architecture**

**Modular Function Pattern**:
```python
def extract_source_table(spark: SparkSession, db_config: dict, table_name: str) -> DataFrame:
    """Extract single table with lenient schema"""
    
def add_bronze_audit_fields(df: DataFrame, source_table: str, batch_id: str) -> DataFrame:
    """Add comprehensive audit trail"""
    
def load_to_bronze_table(df: DataFrame, db_config: dict, bronze_table: str) -> None:
    """Load with full refresh strategy"""
    
def transform_[table]_to_bronze(spark: SparkSession, db_config: dict, batch_id: str) -> Dict[str, int]:
    """Individual table transform with metrics"""
    
def run(spark: SparkSession, db_config: dict, execution_date: str = None) -> Dict[str, any]:
    """Main orchestration function"""
```

### **5. Airflow DAG Patterns**

**Production DAG Structure**:
```python
with DAG(
    dag_id='bronze_basic_pagila_ingestion',
    default_args=DEFAULT_ARGS,
    description='Ingest Pagila source data into Bronze Basic layer',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,
    tags=['bronze', 'basic', 'pagila', 'ingestion']
) as dag:
    
    start_task = DummyOperator(task_id='start_bronze_ingestion')
    
    # Preflight checks (parallel)
    with TaskGroup('preflight_checks') as preflight_group:
        check_source = PythonOperator(task_id='check_source_database', ...)
        check_target = PythonOperator(task_id='check_target_database', ...)
    
    # Main transform
    run_transform = PythonOperator(task_id='run_bronze_transform', ...)
    
    # Validation
    validate_data = PythonOperator(task_id='validate_bronze_data', ...)
    
    # Notification
    notify_completion = PythonOperator(task_id='send_completion_notification', ...)
    
    end_task = DummyOperator(task_id='end_bronze_ingestion')
    
    # Dependencies
    start_task >> preflight_group >> run_transform >> validate_data >> notify_completion >> end_task
```

---

## 📋 Bronze Layer Best Practices

### **1. Data Quality & Resilience**

**✅ Lenient Typing Strategy**:
- Cast ALL source columns to `TEXT/str` in bronze layer
- Handle data quality issues gracefully
- Preserve original values for debugging
- Type conversion happens in silver layer

**✅ Comprehensive Audit Trail**:
```python
# Required bronze audit fields
br_load_time: datetime        # When record was loaded
br_source_file: str          # Source system/file identifier  
br_record_hash: str          # MD5 hash of source columns
br_is_current: bool          # Record currency flag
br_batch_id: str             # Batch/job identifier
br_[table]_key: UUID         # Bronze surrogate key
```

**✅ Error Handling Pattern**:
```python
try:
    rows_processed = transform_func(spark, db_config, batch_id)
    results["tables_processed"].append({
        "table": table_name,
        "metrics": {"rows_processed": rows_processed}
    })
except Exception as e:
    error_msg = f"Failed to process {table_name}: {str(e)}"
    results["errors"].append(error_msg)
    # Continue processing other tables
```

### **2. Performance Optimization**

**✅ Spark 4.0.0 Performance Features**:
- **Adaptive Query Execution (AQE)**: Automatic optimization
- **ANSI SQL Mode**: Better data integrity 
- **Arrow Integration**: Faster Python/Spark data exchange
- **Skew Join Handling**: Automatic skewed data optimization

**✅ JDBC Best Practices**:
```python
# Efficient JDBC loading
(df.write
 .format("jdbc")
 .option("url", target_url)
 .option("dbtable", f"staging_pagila.{bronze_table}")
 .option("user", db_config['user'])
 .option("password", db_config['password'])
 .option("driver", "org.postgresql.Driver")
 .mode("overwrite")  # Full refresh for bronze
 .save())
```

### **3. Configuration Management**

**✅ Database Configuration Pattern**:
```python
db_config = {
    'host': 'host.docker.internal',  # Docker networking
    'port': 5432,
    'user': 'postgres', 
    'password': 'postgres'
}
```

**✅ Environment-Specific Settings**:
- Development: `host.docker.internal` for Docker
- Production: Proper service discovery/DNS
- Use environment variables for credentials

---

## 🚨 Critical Troubleshooting Guide

### **Issue: PySpark JavaPackage Error**
**Root Cause**: Version mismatch between Spark binary and PySpark package  
**Solution**: Ensure Spark 4.0.0 binary matches PySpark 4.0.0 package  
**Prevention**: Use exact version matching in Dockerfile and requirements.txt

### **Issue: JDBC Driver Not Found**
**Root Cause**: Driver not in Spark's classpath  
**Solution**: Copy JDBC JAR to `/opt/spark/jars/` AND configure in SparkSession  
**Prevention**: Include both in Dockerfile and runtime config

### **Issue: Database Connection Failures**
**Root Cause**: Docker networking between Airflow and PostgreSQL  
**Solution**: Use `host.docker.internal` for external PostgreSQL  
**Prevention**: Test connectivity in preflight checks

### **Issue: Java Version Compatibility**
**Root Cause**: Spark 4.0.0 requires Java 17+  
**Solution**: Install OpenJDK 17 in Docker image  
**Prevention**: Document version requirements clearly

---

## 📊 Validation & Monitoring

### **Data Validation Patterns**
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

### **Monitoring Metrics**
```python
results = {
    "batch_id": batch_id,
    "execution_date": execution_date,
    "tables_processed": [{"table": name, "metrics": {...}}],
    "total_rows_processed": 33898,
    "errors": []
}
```

---

## 🔄 Replication Guide for bronze_cdc

### **1. Copy Core Structure**
```bash
# Copy from bronze_basic to bronze_cdc
cp -r src/bronze_basic/transforms/ src/bronze_cdc/transforms/
cp src/bronze_basic/CLAUDE.md src/bronze_cdc/
cp airflow/dags/bronze/bronze_basic_pagila_ingestion.py airflow/dags/bronze/bronze_cdc_ingestion.py
```

### **2. Adapt for CDC Patterns**
- **Change table models** to CDC structure (lsn, op, full_row, load_ts)
- **Update transform logic** for CDC extraction from WAL
- **Modify validation** for CDC-specific patterns
- **Adjust naming** (br_customer_cdc vs br_customer)

### **3. Key Differences for CDC**
```python
# CDC table structure
class BrCustomerCdc(SQLModel, table=True):
    __tablename__ = "br_customer_cdc"
    
    # CDC-specific fields
    lsn: int                    # Log Sequence Number
    op: str                     # Operation (I/U/D)
    full_row: dict             # Complete record as JSON
    load_ts: datetime          # CDC load timestamp
    
    # Standard bronze audit fields (same as basic)
    br_load_time: datetime
    br_batch_id: str
    # ... etc
```

### **4. Preserve Successful Patterns**
- **Spark 4.0.0 configuration** (copy exactly)
- **Docker setup** (reuse same Dockerfile)
- **Error handling patterns** (copy exception handling)
- **Validation logic** (adapt validation checks)
- **Airflow DAG structure** (reuse task groups and dependencies)

---

## 📚 Reference Documentation

### **Key Files to Reference**
- `src/bronze_basic/transforms/pagila_to_bronze.py` - Transform implementation
- `airflow/dags/bronze/bronze_basic_pagila_ingestion.py` - DAG orchestration  
- `docker/airflow/Dockerfile` - Environment setup
- `src/bronze_basic/tables/staging_pagila/br_*.py` - Table models

### **Spark 4.0.0 Documentation**
- [Apache Spark 4.0.0 Release Notes](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [PySpark 4.0.0 API Documentation](https://spark.apache.org/docs/latest/api/python/index.html)
- [Spark SQL ANSI Mode Guide](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)

### **Production Deployment Checklist**
- [ ] Spark 4.0.0 + PySpark 4.0.0 versions match
- [ ] Java 17+ installed and configured  
- [ ] JDBC drivers in Spark classpath
- [ ] Database connectivity tested
- [ ] Audit fields populated correctly
- [ ] Error handling tested
- [ ] Monitoring and alerting configured
- [ ] Data validation rules implemented

---

## 📚 Troubleshooting & Lessons Learned

### **Critical Issues Reference**
For detailed troubleshooting of common bronze layer issues, see [FAQ.md](FAQ.md):

- **Memory Configuration**: Spark 4.0.0 minimum requirements (471MB+)
- **Docker Resource Limits**: Container memory allocation strategies
- **SIGKILL Errors**: Process termination and recovery procedures
- **Performance Optimization**: Benchmarks and monitoring guidelines

### **Key Takeaways from Production Issues**
1. **Spark 4.0.0 Memory Requirements**: Always allocate minimum 512MB for driver and executor
2. **Container Resource Planning**: Docker containers need 2GB+ for Spark workloads
3. **Configuration Consistency**: Use explicit values rather than environment variables for critical settings
4. **Monitoring Approach**: Watch for zombie jobs, SIGKILL signals, and memory usage patterns

---

---

## 🧬 Evolution Tracking DNA (CRITICAL)

**Every bronze_basic development session MUST update evolution documentation.**

📚 **CENTRALIZED STANDARDS**: All evolution tracking requirements are defined in:

👉 **[`docs/EVOLUTION_TRACKING_STANDARDS.md`](../../docs/EVOLUTION_TRACKING_STANDARDS.md)** 👈

### **Bronze Basic Specific Requirements**:

**Layer Changelog**: `src/bronze_basic/CHANGELOG.md` - REQUIRED after ANY changes

**Key Metrics to Track**:
- Processing rate (rows/second throughput)
- Memory stability patterns  
- Error recovery success rate
- Spark 4.0.0 compatibility lessons
- Infrastructure evolution (Docker, JDBC, configurations)

**Focus Areas for Bronze Basic Evolution**:
- Technical decisions (Spark configs, Docker setup, JDBC optimizations)
- Performance patterns (processing rates, memory usage, error rates)
- Troubleshooting solutions (common errors and resolutions)
- Infrastructure compatibility (Spark/Java/Python version matrix)

**📖 For complete templates, automation, and standards, see [`docs/EVOLUTION_TRACKING_STANDARDS.md`](../../docs/EVOLUTION_TRACKING_STANDARDS.md)**

---

**🎯 Success Criteria**: When replicating this pattern, you should achieve similar results:
- All DAG tasks pass (green checkmarks)
- Data loaded with proper audit trails
- No version compatibility issues
- Comprehensive error handling
- Production-ready architecture
- **Evolution documentation updated with lessons learned**

**🚀 Next Steps**: Use this as a template for bronze_cdc, adapting the CDC-specific patterns while preserving the proven technical foundation. **CRITICAL**: Update evolution documentation to track adaptations and improvements made for CDC implementation.
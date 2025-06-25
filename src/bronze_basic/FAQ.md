# 🥉 Bronze Basic Layer - FAQ & Troubleshooting Guide

> **Purpose**: Document critical issues, solutions, and lessons learned from bronze basic layer implementation  
> **Audience**: Data engineers troubleshooting bronze ingestion pipeline  
> **Last Updated**: 2025-06-24

---

## 🚨 Critical Issues & Solutions

### **Issue 1: Spark Memory Configuration (CRITICAL)**

**Problem**: Bronze DAGs failing with zombie job errors and SIGKILL signals
```
System memory 268435456 must be at least 471859200
```

**Root Cause**: Spark 4.0.0 requires minimum 471MB but was configured for 256MB

**Solution**: Update Spark memory configuration in DAG
```python
# In bronze_basic_pagila_ingestion.py
.config("spark.executor.memory", "512m")
.config("spark.driver.memory", "512m")
.config("spark.driver.maxResultSize", "256m")
```

**Prevention**: Always verify Spark 4.0.0 memory requirements before deployment

---

### **Issue 2: Docker Container Memory Limits**

**Problem**: Tasks killed with exit code 137 (SIGKILL) due to memory constraints

**Root Cause**: Docker containers limited to 1GB but Spark required more memory

**Solution**: Update docker-compose.yml
```yaml
# Increase memory limits
memory: 2G
```

**Prevention**: Monitor container memory usage and set limits appropriately

---

### **Issue 3: Environment Variable vs Hardcoded Configuration**

**Problem**: Inconsistent Spark configuration between environment variables and DAG settings

**Root Cause**: DAG reading from environment variables that didn't match actual requirements

**Solution**: Use hardcoded values in DAG for consistency
```python
# Replace environment variable usage with explicit values
.config("spark.executor.memory", "512m")  # Instead of os.getenv()
```

**Prevention**: Document configuration explicitly in code rather than relying on environment

---

## 🔧 Configuration Best Practices

### **Spark 4.0.0 Memory Settings**
```python
# Recommended bronze layer Spark configuration
spark = (SparkSession.builder
         .appName("Bronze_Basic_Ingestion")
         .config("spark.executor.memory", "512m")
         .config("spark.driver.memory", "512m") 
         .config("spark.driver.maxResultSize", "256m")
         .config("spark.executor.cores", "1")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .master("local[2]")
         .getOrCreate())
```

### **Docker Resource Allocation**
```yaml
# docker-compose.yml
services:
  airflow-webserver:
    deploy:
      resources:
        limits:
          memory: 2G
        reservations:
          memory: 1G
```

---

## 📊 Performance Benchmarks

### **Expected Performance**
- **Bronze ingestion**: ~35K records in <10 minutes
- **Memory usage**: Peak 512MB per Spark executor
- **Container overhead**: ~1.5GB total memory per container

### **Success Indicators**
- ✅ No SIGKILL errors in container logs
- ✅ Spark jobs complete without zombie processes
- ✅ Bronze tables populated with expected record counts
- ✅ DAG completes within SLA timeframes

---

## 🔍 Diagnostic Commands

### **Check Container Memory Usage**
```bash
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

### **Verify Bronze Data**
```sql
-- Check bronze table record counts
SELECT 
    schemaname,
    tablename,
    n_tup_ins as records
FROM pg_stat_user_tables 
WHERE schemaname = 'staging_pagila'
ORDER BY tablename;
```

### **Monitor Spark Application**
```bash
# Check Spark application logs
docker logs <airflow_container> | grep "SparkSession\|Memory\|Error"
```

---

## ⚠️ Warning Signs

### **Memory Issues**
- Container restart frequency > 10% of runs
- Spark driver OOM exceptions
- Task failure with exit code 137

### **Performance Degradation**
- Bronze ingestion time > 15 minutes
- High CPU usage (>80%) sustained
- Memory usage approaching container limits

---

## 🚀 Quick Fixes

### **Emergency Memory Fix**
```bash
# Temporary fix: Increase Docker memory via docker-compose
docker-compose down
# Edit docker-compose.yml memory limits to 2G
docker-compose up -d
```

### **Verify Fix Applied**
```python
# In Python/Airflow logs, look for:
# "SparkSession created successfully"
# "Bronze transformation completed: {'total_bronze_rows_processed': 35000+}"
```

---

## 📚 Related Documentation

- **Main CLAUDE.md**: Section 7 - Python Virtual Environment & Code Execution
- **Docker Configuration**: docker-compose.yml memory and resource limits
- **Spark Documentation**: [Apache Spark 4.0.0 Memory Configuration](https://spark.apache.org/docs/4.0.0/configuration.html#memory-management)

---

**🔑 Key Takeaway**: Bronze layer stability depends on proper Spark memory configuration and Docker resource allocation. Always test with production-like data volumes and monitor memory usage patterns.
# 🚀 Performance Configuration Guide

> **Purpose**: Comprehensive guide for optimizing medallion lakehouse performance across different machine capabilities  
> **Audience**: Data engineers and DevOps teams deploying the platform  
> **Scope**: Development, testing, and production environments

---

## 🎯 Quick Start

### **Automatic Configuration (Recommended)**
```bash
# Auto-detect and apply optimal configuration
python scripts/performance-config.py --auto

# Check recommendations without applying
python scripts/performance-config.py
```

### **Manual Configuration**
```bash
# High-performance (16GB+ RAM, 8+ cores)
python scripts/performance-config.py --config performance

# Balanced (12GB+ RAM, 6+ cores) 
python scripts/performance-config.py --config balanced

# Lite (8GB- RAM, 4- cores, CI/testing)
python scripts/performance-config.py --config lite
```

---

## 📊 Performance Tiers

### 🚀 **High-Performance Configuration**
**Target**: High-end workstations, desktop development rigs (>32GB RAM, 12+ cores)

```yaml
Resources:
  - Scheduler Container: 8GB RAM, 4 CPUs
  - Spark Driver: 2GB RAM
  - Spark Executor: 4GB RAM
  - Expected DAG Time: 20-30 seconds
  
Features:
  - Parallel table processing
  - Advanced Spark optimizations (AQE, skew joins)
  - Arrow integration enabled
  - G1 garbage collector
```

**Use Cases:**
- High-volume data processing
- Complex analytical queries
- Large dataset processing
- Performance benchmarking

### ⚖️ **Balanced Configuration**
**Target**: Better developer machines and small workstations (16-32GB RAM, 8+ cores)

```yaml
Resources:
  - Scheduler Container: 4GB RAM, 2 CPUs  
  - Spark Driver: 1GB RAM
  - Spark Executor: 2GB RAM
  - Expected DAG Time: 30-45 seconds

Features:
  - Batch processing strategy
  - Moderate Spark optimizations
  - Arrow integration enabled
  - Balanced resource usage
```

**Use Cases:**
- Regular development work
- Mid-size dataset processing
- Team development environments
- Code review and testing

### 💡 **Lite Configuration** 
**Target**: Most developer laptops, CI/CD environments (≤16GB RAM)

```yaml
Resources:
  - Scheduler Container: 2GB RAM, 2 CPUs
  - Spark Driver: 512MB RAM  
  - Spark Executor: 512MB RAM
  - Expected DAG Time: 45-60 seconds

Features:
  - Sequential processing
  - Conservative memory usage
  - Arrow integration disabled for stability
  - Serial garbage collector
```

**Use Cases:**
- Daily development on standard laptops
- CI/CD pipelines
- Testing and validation
- Resource-constrained environments

---

## 🔧 Configuration Files

### **Docker Compose Overrides**

The system uses Docker Compose override files for configuration:

```bash
# Base configuration (balanced)
docker-compose.yml

# High-performance overlay
docker-compose.performance.yml  

# Lite overlay  
docker-compose.lite.yml
```

### **Automatic Usage**
```bash
# Performance tier
docker-compose -f docker-compose.yml -f docker-compose.performance.yml up -d

# Lite tier
docker-compose -f docker-compose.yml -f docker-compose.lite.yml up -d

# Balanced tier (default)
docker-compose up -d
```

### **Environment Variables**

Override automatic detection:

```bash
# Force specific tier for all processes
export MEDALLION_PERFORMANCE_TIER=performance

# Individual Spark settings
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=4g
```

---

## 🎛️ DAG Integration

### **Performance-Aware DAGs**

DAGs automatically adapt to the detected performance tier:

```python
from scripts.spark_config_generator import get_spark_session, get_current_tier

def run_transform(**context):
    # Get optimized Spark session for current tier
    spark = get_spark_session(f"MyTransform_{context['ds']}")
    
    # Adapt processing strategy based on tier
    tier = get_current_tier()
    if tier == "performance":
        # Process all tables in parallel
        tables = ["table1", "table2", "table3", "table4"]
    elif tier == "balanced":
        # Process in batches
        tables = ["table1", "table2"]
    else:  # lite
        # Process one at a time
        tables = ["table1"]
    
    # ... rest of transform logic
```

### **Existing DAGs**

Update existing DAGs to use dynamic configuration:

```python
# OLD: Hardcoded configuration
spark = (SparkSession.builder
         .appName("MyApp")
         .config("spark.driver.memory", "1g")
         .config("spark.executor.memory", "1g")
         .getOrCreate())

# NEW: Performance-aware configuration  
from scripts.spark_config_generator import get_spark_session
spark = get_spark_session("MyApp")
```

---

## 📈 Performance Benchmarks

### **Bronze Layer Processing (35K records)**

| Configuration | DAG Time | Memory Usage | CPU Usage | Parallel Tasks |
|---------------|----------|--------------|-----------|----------------|
| Performance   | 20-30s   | ~6GB peak   | ~300%     | 4-6           |
| Balanced      | 30-45s   | ~3GB peak   | ~200%     | 2-3           |
| Lite          | 45-60s   | ~1.5GB peak | ~100%     | 1             |

### **Silver-Gold Pipeline (1,605 records)**

| Configuration | End-to-End | Silver Time | Gold Time | Success Rate |
|---------------|------------|-------------|-----------|--------------|
| Performance   | 25-35s     | 10-15s     | 15-20s    | 100%         |
| Balanced      | 35-50s     | 15-25s     | 20-30s    | 100%         |
| Lite          | 50-70s     | 25-35s     | 25-40s    | 100%         |

---

## 🔍 Monitoring & Diagnostics

### **System Resource Monitoring**
```bash
# Check current resource usage
docker stats --no-stream

# Monitor Spark jobs
docker-compose logs airflow-scheduler --tail 50

# Check detected configuration
python scripts/performance-config.py
```

### **Performance Metrics**
```bash
# DAG execution times
grep "DagRun Finished" logs/dag_id=*/run_id=*/attempt=*.log

# Memory usage patterns  
grep "Memory" logs/dag_id=*/run_id=*/attempt=*.log

# Error patterns
grep -E "(SIGKILL|OutOfMemory|Failed)" logs/dag_id=*/run_id=*/attempt=*.log
```

---

## 🚨 Troubleshooting

### **Common Issues by Tier**

#### **Performance Tier Issues**
- **Memory Exhaustion**: Reduce parallel tasks or lower Spark memory
- **CPU Saturation**: Limit executor cores or reduce concurrent DAGs
- **Disk I/O**: Consider SSD or reduce shuffle operations

#### **Balanced Tier Issues**  
- **Slower Than Expected**: Check for background processes consuming resources
- **Memory Pressure**: Monitor swap usage, consider upgrading to performance tier
- **Intermittent Failures**: May need to tune batch sizes

#### **Lite Tier Issues**
- **Very Slow Execution**: Expected behavior, consider hardware upgrade
- **Memory Errors**: Further reduce Spark memory or disable complex features
- **Container Restarts**: Check Docker memory limits vs system available memory

### **Switching Between Configurations**

```bash
# Stop current environment
docker-compose down

# Switch to different tier
python scripts/performance-config.py --config [lite|balanced|performance]

# Or use docker-compose directly
docker-compose -f docker-compose.yml -f docker-compose.performance.yml up -d
```

### **Emergency Fallback**
```bash
# If performance tier fails, fallback to lite
python scripts/performance-config.py --config lite

# Or minimal manual startup
docker-compose -f docker-compose.yml -f docker-compose.lite.yml up -d
```

---

## 🎯 Best Practices

### **Development Workflow**
1. **Start with auto-detection**: Let the system choose optimal configuration
2. **Monitor performance**: Use `docker stats` and Airflow logs
3. **Adjust as needed**: Switch tiers based on workload requirements
4. **Test on target environment**: Use same configuration as production

### **CI/CD Integration**
```bash
# Always use lite configuration in CI
export MEDALLION_PERFORMANCE_TIER=lite
python scripts/performance-config.py --auto
```

### **Production Deployment**
1. **Benchmark all tiers** on production hardware
2. **Monitor resource utilization** over time
3. **Scale horizontally** rather than just vertically
4. **Document team standard** configuration

### **Team Standards**
```bash
# Team configuration file
echo "performance" > .team-config

# Everyone uses the same tier
python scripts/performance-config.py --config $(cat .team-config)
```

---

## 📚 Reference

### **Key Files**
- `scripts/performance-config.py` - Main configuration management
- `scripts/spark-config-generator.py` - Dynamic Spark configuration
- `docker-compose.performance.yml` - High-performance overlay
- `docker-compose.lite.yml` - Resource-constrained overlay
- `examples/performance-aware-dag.py` - Example adaptive DAG

### **Environment Variables**
- `MEDALLION_PERFORMANCE_TIER` - Force specific tier (lite|balanced|performance)
- `SPARK_DRIVER_MEMORY` - Override driver memory
- `SPARK_EXECUTOR_MEMORY` - Override executor memory
- `SPARK_DRIVER_MAX_RESULT_SIZE` - Override result size limit

### **Docker Commands**
```bash
# Check current configuration
cat .current-config

# Manual tier startup
docker-compose -f docker-compose.yml -f docker-compose.${TIER}.yml up -d

# Resource monitoring
watch "docker stats --no-stream"
```

---

**🎯 Success Criteria**: Choose and apply the configuration that gives you:
- Reliable DAG execution (100% success rate)
- Acceptable performance for your workflow
- Stable resource utilization
- Team productivity optimization

**🚀 Next Steps**: Start with auto-detection, monitor performance, and adjust based on your specific workload and hardware capabilities.
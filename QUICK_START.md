# 🚀 Quick Start Guide

## ⚡ One-Command Setup

Choose your configuration based on your machine:

### **Most Developer Laptops (≤16GB RAM)**
```bash
./scripts/simple-performance-config.sh lite
```

### **Better Developer Machines (16-32GB RAM)**
```bash
./scripts/simple-performance-config.sh balanced
```

### **High-End Workstations (>32GB RAM)**
```bash
./scripts/simple-performance-config.sh performance
```

### **Auto-Detect (Recommended)**
```bash
./scripts/simple-performance-config.sh auto
```

---

## 🔍 Check Your System

```bash
./scripts/simple-performance-config.sh status
```

---

## 🎯 After Setup

1. **Wait 30-60 seconds** for services to start
2. **Open Airflow UI**: http://localhost:8080 (admin/admin)
3. **Test the pipeline**: Trigger `bronze_basic_pagila_ingestion` DAG
4. **Check results**: Should complete in 45-60 seconds (lite) to 20-30 seconds (performance)

---

## 🔄 Switch Configurations

```bash
# Switch to different configuration anytime
./scripts/simple-performance-config.sh [lite|balanced|performance]

# Check what's currently running
./scripts/simple-performance-config.sh status
```

---

## ❓ Troubleshooting

**DAG fails with memory errors?**
```bash
./scripts/simple-performance-config.sh lite
```

**Too slow?**
```bash
./scripts/simple-performance-config.sh balanced
# or
./scripts/simple-performance-config.sh performance
```

**Check detailed troubleshooting**: See [PERFORMANCE_GUIDE.md](PERFORMANCE_GUIDE.md)

---

**🎯 Success**: Your medallion lakehouse should be running smoothly with optimal performance for your machine!
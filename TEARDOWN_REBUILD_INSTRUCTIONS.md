# 🔄 Complete Teardown & Rebuild Instructions

## 📋 **Prerequisites**
- Pagila database is in separate repo: `/mnt/c/repos/pagila/`
- This medallion demo connects to that external Pagila instance
- Airflow and medallion databases are built by this repo's orchestration scripts

---

## 🧹 **COMPLETE TEARDOWN PROCESS**

### Step 1: Tear Down Medallion Airflow Environment
```bash
# From /mnt/c/repos/sqlmodel-medallion-demo/
docker compose down
docker system prune -f
docker volume prune -f
```

### Step 2: Tear Down External Pagila Database
```bash
# From /mnt/c/repos/pagila/
cd /mnt/c/repos/pagila
docker compose down
docker volume prune -f
```

---

## 🏗️ **COMPLETE REBUILD PROCESS**

### Step 1: Start External Pagila Database (FIRST!)
```bash
# From /mnt/c/repos/pagila/
cd /mnt/c/repos/pagila
docker compose up -d
# Wait for database to be ready
sleep 30
```

### Step 2: Start Medallion Airflow Environment
```bash
# From /mnt/c/repos/sqlmodel-medallion-demo/
cd /mnt/c/repos/sqlmodel-medallion-demo
docker compose up -d
# Wait for containers to be healthy
sleep 60
```

### Step 3: Build All Medallion Databases & Schemas
```bash
# Build all databases using orchestration
.venv/Scripts/python.exe scripts/orchestrate_build.py

# This will build:
# - bronze_basic database with schemas and tables
# - bronze_cdc database with schemas and tables  
# - All other medallion layer databases
```

### Step 4: Verify Environment is Ready
```bash
# Check containers are running
docker ps

# Check databases exist
docker exec pagila psql -U postgres -l

# Check Airflow is accessible
curl -f http://localhost:8080/health
```

---

## 🎯 **TESTING READY STATE**

After rebuild, test both bronze layers:

### Test Bronze Basic
```bash
# Should see DAG in Airflow UI
docker exec medallion-airflow-webserver airflow dags list | grep bronze_basic

# Trigger and verify
docker exec medallion-airflow-webserver airflow dags trigger bronze_basic_pagila_ingestion
```

### Test Bronze CDC  
```bash
# Should see DAG in Airflow UI
docker exec medallion-airflow-webserver airflow dags list | grep bronze_cdc

# Trigger and verify
docker exec medallion-airflow-webserver airflow dags trigger bronze_cdc_ingestion
```

---

## 🚨 **CRITICAL DEPENDENCIES**

1. **Pagila MUST be running first** - this repo connects to external Pagila at `host.docker.internal:5432`
2. **Network connectivity** - Airflow containers must reach external PostgreSQL
3. **Database orchestration** - Run orchestrate_build.py to create all schemas/tables
4. **Container health** - Wait for healthy status before proceeding

---

## 📊 **SUCCESS INDICATORS**

- [ ] Pagila database running with sample data
- [ ] Airflow webserver accessible at localhost:8080
- [ ] bronze_basic and bronze_cdc databases exist with proper schemas
- [ ] Both bronze DAGs visible and executable in Airflow UI
- [ ] End-to-end data flow from Pagila → Bronze layers working

**This process ensures a completely clean, repeatable rebuild from scratch.**
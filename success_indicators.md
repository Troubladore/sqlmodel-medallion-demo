# 🎯 Bronze DAG Success Indicators

## **Immediate Success Signs**

### 1. **Airflow Web UI (http://localhost:8080)**
- ✅ DAG appears with green "success" status
- ✅ All tasks show green checkmarks
- ✅ Run duration shows reasonable time (2-10 minutes)
- ✅ No red "failed" or orange "retry" indicators

### 2. **Data Loaded Successfully**
```bash
# Quick row count check
docker exec pagila psql -U postgres -d bronze_basic -c "
SELECT 
  'br_actor' as table, COUNT(*) as rows FROM staging_pagila.br_actor 
UNION ALL SELECT 
  'br_customer', COUNT(*) FROM staging_pagila.br_customer
UNION ALL SELECT 
  'br_film', COUNT(*) FROM staging_pagila.br_film;
"
```

**Expected Results:**
- br_actor: ~200 rows  
- br_customer: ~599 rows
- br_film: ~1000 rows

### 3. **Audit Fields Populated**
```bash
# Check audit fields are working
docker exec pagila psql -U postgres -d bronze_basic -c "
SELECT 
  br_batch_id,
  br_load_time,
  COUNT(*) as records
FROM staging_pagila.br_actor 
GROUP BY br_batch_id, br_load_time;
"
```

**Expected Results:**
- All records have same `br_batch_id` (format: bronze_basic_YYYYMMDD_HHMMSS)
- All records have recent `br_load_time` 
- `br_record_hash` values are populated (not NULL)

## **Detailed Success Verification**

### 4. **Source Data Comparison**
```bash
# Compare source vs bronze counts
echo "Source counts:"
docker exec pagila psql -U postgres -d pagila -c "
SELECT 'actor' as table, COUNT(*) FROM actor 
UNION ALL SELECT 'customer', COUNT(*) FROM customer;
"

echo "Bronze counts:"  
docker exec pagila psql -U postgres -d bronze_basic -c "
SELECT 'br_actor' as table, COUNT(*) FROM staging_pagila.br_actor
UNION ALL SELECT 'br_customer', COUNT(*) FROM staging_pagila.br_customer;
"
```

**Expected:** Bronze counts should match source counts exactly.

### 5. **Data Quality Checks**
```bash
# Check for data quality issues
docker exec pagila psql -U postgres -d bronze_basic -c "
SELECT 
  COUNT(*) as total_rows,
  COUNT(CASE WHEN actor_id IS NOT NULL THEN 1 END) as valid_actor_ids,
  COUNT(CASE WHEN br_record_hash IS NOT NULL THEN 1 END) as valid_hashes,
  COUNT(CASE WHEN br_is_current = true THEN 1 END) as current_records
FROM staging_pagila.br_actor;
"
```

**Expected:** All counts should be equal (100% data quality).

## **Error Indicators to Watch For**

### ❌ **Failure Signs**
- Red failed tasks in Airflow UI
- Empty bronze tables after DAG completion
- NULL values in required audit fields
- Row count mismatches between source and bronze
- Missing `br_batch_id` or `br_load_time` values

### ⚠️ **Warning Signs**  
- Orange retry indicators (temporary issues)
- Partial data loads (some tables empty, others populated)
- Very fast completion time (<30 seconds - indicates no data processed)
- Very slow completion time (>15 minutes - indicates performance issues)

## **Monitoring Commands**

```bash
# Real-time monitoring during execution
./monitor_bronze_dag.sh

# Post-execution verification  
./verify_bronze_data.sh

# Check specific DAG run
docker-compose exec airflow-webserver airflow tasks states-for-dag-run bronze_basic_pagila_ingestion 2025-06-23T20:29:54 --output table
```

## **Expected Timeline**
- **Start to Preflight Checks:** 1-2 minutes
- **Preflight to Transform Start:** 30 seconds  
- **Transform Execution:** 3-8 minutes (depends on data size)
- **Validation:** 1-2 minutes
- **Total Runtime:** 5-12 minutes

When successful, you'll see data flowing from the source Pagila tables into the bronze layer with full audit trails and data quality tracking!
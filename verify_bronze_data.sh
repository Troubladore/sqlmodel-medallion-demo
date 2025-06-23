#!/bin/bash
# Verify Bronze Layer Data Loading
# Run this after DAG completes to verify data was loaded

echo "🔍 BRONZE LAYER DATA VERIFICATION"
echo "=================================="

echo ""
echo "📊 Table Row Counts:"
echo "-------------------"
docker exec pagila psql -U postgres -d bronze_basic -c "
SELECT 
    schemaname,
    tablename,
    n_tup_ins as row_count,
    n_tup_upd as updates,
    n_tup_del as deletes
FROM pg_stat_user_tables 
WHERE schemaname = 'staging_pagila'
ORDER BY tablename;
"

echo ""
echo "📅 Recent Load Times (Last 5 Batches):"
echo "-------------------------------------"
docker exec pagila psql -U postgres -d bronze_basic -c "
SELECT DISTINCT
    br_batch_id,
    br_load_time,
    COUNT(*) as records
FROM staging_pagila.br_actor
GROUP BY br_batch_id, br_load_time
ORDER BY br_load_time DESC
LIMIT 5;
"

echo ""
echo "🔍 Sample Data from Each Table:"
echo "------------------------------"
for table in br_actor br_customer br_film br_language br_payment br_rental; do
    echo "--- $table ---"
    docker exec pagila psql -U postgres -d bronze_basic -c "
    SELECT COUNT(*) as total_rows,
           COUNT(CASE WHEN br_is_current THEN 1 END) as current_rows,
           MAX(br_load_time) as latest_load
    FROM staging_pagila.$table;
    "
done

echo ""
echo "⚠️  Data Quality Check:"
echo "----------------------"
docker exec pagila psql -U postgres -d bronze_basic -c "
SELECT 
    'br_actor' as table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN actor_id IS NULL THEN 1 END) as null_source_ids,
    COUNT(CASE WHEN br_record_hash IS NULL THEN 1 END) as missing_hashes
FROM staging_pagila.br_actor
UNION ALL
SELECT 
    'br_customer' as table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_source_ids,
    COUNT(CASE WHEN br_record_hash IS NULL THEN 1 END) as missing_hashes
FROM staging_pagila.br_customer;
"
#!/bin/bash
# Comprehensive Bronze DAG Status Check
# Usage: ./check_bronze_status.sh

echo "🏗️ BRONZE INGESTION STATUS DASHBOARD"
echo "===================================="

echo ""
echo "📊 DAG Status:"
docker-compose exec airflow-webserver airflow dags list | grep bronze_basic_pagila_ingestion

echo ""
echo "🕐 Recent DAG Runs:"
docker-compose exec airflow-webserver airflow dags list-runs --dag-id bronze_basic_pagila_ingestion --output table 2>/dev/null | head -10

echo ""
echo "📋 Current Data in Bronze Tables:"
docker exec pagila psql -U postgres -d bronze_basic -c "
SELECT 
  'br_actor' as table_name, COUNT(*) as row_count FROM staging_pagila.br_actor 
UNION ALL SELECT 
  'br_customer', COUNT(*) FROM staging_pagila.br_customer
UNION ALL SELECT 
  'br_film', COUNT(*) FROM staging_pagila.br_film
UNION ALL SELECT
  'br_language', COUNT(*) FROM staging_pagila.br_language
UNION ALL SELECT
  'br_payment', COUNT(*) FROM staging_pagila.br_payment  
UNION ALL SELECT
  'br_rental', COUNT(*) FROM staging_pagila.br_rental
ORDER BY table_name;
"

echo ""
echo "💡 Next Steps:"
echo "  1. Open http://localhost:8080 to view Airflow UI"
echo "  2. Look for bronze_basic_pagila_ingestion DAG"
echo "  3. Check for green success indicators"
echo "  4. Run './verify_bronze_data.sh' for detailed verification"
echo ""
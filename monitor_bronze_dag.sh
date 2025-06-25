#!/bin/bash
# Monitor Bronze DAG Execution
# Usage: ./monitor_bronze_dag.sh [execution_date]

EXECUTION_DATE=${1:-$(date '+%Y-%m-%dT%H:%M:%S')}
DAG_ID="bronze_basic_pagila_ingestion"

echo "🔍 Monitoring Bronze DAG Run: $EXECUTION_DATE"
echo "=============================================="

# Check overall DAG run status
echo "📊 DAG Run Status:"
docker-compose exec airflow-webserver airflow dags state $DAG_ID $EXECUTION_DATE

echo ""
echo "📋 Task Status Overview:"
docker-compose exec airflow-webserver airflow tasks states-for-dag-run $DAG_ID $EXECUTION_DATE --output table

echo ""
echo "🕐 Checking for recent runs..."
docker-compose exec airflow-webserver airflow dags list-runs --dag-id $DAG_ID --limit 3 --output table
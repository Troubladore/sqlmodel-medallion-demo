"""
🥉 BRONZE BASIC - Simplified Pagila to Bronze Transform (No PySpark)

Purpose: Extract data from source Pagila database and load into bronze layer
Architecture: Direct SQL operations using SQLAlchemy (PySpark-free for demo)

This simplified version bypasses PySpark Java configuration issues while 
demonstrating the same bronze layer patterns and audit fields.
"""

import hashlib
import uuid
from datetime import datetime
from typing import Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text


def extract_and_load_table(source_engine, target_engine, table_name: str, batch_id: str) -> int:
    """Extract from source table and load to bronze with audit fields"""
    
    # Extract data from source
    query = f"SELECT * FROM public.{table_name}"
    source_df = pd.read_sql(query, source_engine)
    
    if len(source_df) == 0:
        return 0
    
    # Add bronze audit fields
    source_df['br_load_time'] = datetime.now()
    source_df['br_source_file'] = f"pagila.public.{table_name}"
    source_df['br_batch_id'] = batch_id
    source_df['br_is_current'] = True
    
    # Create record hash from source columns
    source_cols = [c for c in source_df.columns if not c.startswith('br_')]
    source_df['br_record_hash'] = source_df[source_cols].apply(
        lambda row: hashlib.md5('|'.join(str(row.values)).encode()).hexdigest(), 
        axis=1
    )
    
    # Add bronze surrogate key
    source_df[f'br_{table_name}_key'] = [str(uuid.uuid4()) for _ in range(len(source_df))]
    
    # Convert all source columns to strings (bronze lenient typing)
    for col in source_cols:
        source_df[col] = source_df[col].astype(str)
    
    # Load to bronze table
    bronze_table = f"br_{table_name}"
    source_df.to_sql(
        name=bronze_table,
        con=target_engine,
        schema='staging_pagila',
        if_exists='replace',  # Full refresh strategy
        index=False,
        method='multi'
    )
    
    return len(source_df)


def run_simplified_bronze_transform(db_config: dict, execution_date: str = None) -> Dict[str, Any]:
    """
    Simplified bronze transform using direct SQL operations
    
    Args:
        db_config: Database configuration dictionary
        execution_date: Airflow execution date (optional)
    
    Returns:
        Dict with processing metrics and results
    """
    
    # Generate batch ID
    batch_id = f"bronze_basic_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if execution_date:
        batch_id = f"bronze_basic_{execution_date.replace('-', '').replace(':', '').replace('T', '_')}"
    
    # Create database connections
    source_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/pagila"
    target_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/bronze_basic"
    
    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)
    
    # Define source tables to process
    source_tables = ['actor', 'customer', 'film', 'language', 'payment', 'rental']
    
    # Process each table
    results = {
        "batch_id": batch_id,
        "execution_date": execution_date or datetime.now().isoformat(),
        "tables_processed": [],
        "total_rows_processed": 0,
        "errors": []
    }
    
    for table_name in source_tables:
        try:
            rows_processed = extract_and_load_table(
                source_engine, target_engine, table_name, batch_id
            )
            
            results["tables_processed"].append({
                "table": table_name,
                "rows_processed": rows_processed
            })
            results["total_rows_processed"] += rows_processed
            
            print(f"✅ Processed {table_name}: {rows_processed} rows")
            
        except Exception as e:
            error_msg = f"Failed to process {table_name}: {str(e)}"
            results["errors"].append(error_msg)
            print(f"❌ {error_msg}")
    
    # Close connections
    source_engine.dispose()
    target_engine.dispose()
    
    return results


if __name__ == "__main__":
    # For local testing
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres'
    }
    
    results = run_simplified_bronze_transform(db_config)
    print(f"Transform completed: {results}")
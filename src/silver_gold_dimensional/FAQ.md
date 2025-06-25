# 🥈🥇 Silver-Gold Dimensional Layer - FAQ & Troubleshooting Guide

> **Purpose**: Document critical issues, solutions, and lessons learned from silver-gold dimensional modeling implementation  
> **Audience**: Data engineers troubleshooting dimensional modeling pipeline  
> **Last Updated**: 2025-06-24

---

## 🚨 Critical Issues & Solutions

### **Issue 1: JDBC Void Type Error (CRITICAL)**

**Problem**: Silver tables failing to create with error:
```
Failed to process silver address: Can't get JDBC type for void.
Failed to process silver store: Can't get JDBC type for void.
```

**Root Cause**: `lit(None)` creates void types that JDBC cannot handle in Spark 4.0.0

**Solution**: Explicit type casting in `add_silver_audit_fields` function
```python
# Before (BROKEN):
bronze_key_col = lit(None)

# After (FIXED):
bronze_key_col = lit(None).cast(StringType())
```

**Location**: `src/silver_gold_dimensional/transforms/bronze_to_silver_transforms.py:85`

**Prevention**: Always cast `lit(None)` to explicit types when writing to JDBC

---

### **Issue 2: Date Dimension BIGINT Type Mismatch**

**Problem**: Gold transformation failing with date dimension generation error:
```
Cannot resolve "date_add(1900-01-01, (date_ordinal - 693594))" due to data type mismatch: 
The second parameter requires ("INT" or "SMALLINT" or "TINYINT") type, however "(date_ordinal - 693594)" has the type "BIGINT"
```

**Root Cause**: Spark 4.0.0 stricter type checking for date arithmetic operations

**Solution**: Explicit type casting in date calculations
```python
# Cast BIGINT result to INT for date_add function
.withColumn("calendar_date", 
    expr("date_add('1900-01-01', cast((date_ordinal - 693594) as int))"))
```

**Prevention**: Cast arithmetic results to expected types in Spark 4.0.0

---

### **Issue 3: Ambiguous Column References**

**Problem**: Gold transformations failing with ambiguous column errors:
```
Reference 'last_update' is ambiguous, could be: ['last_update', 'last_update']
```

**Root Cause**: Multiple tables in joins containing same column names without aliases

**Solution**: Use explicit aliases in join operations
```python
# Before (BROKEN):
customer_df.join(address_df, "address_id")

# After (FIXED):
customer_df.alias("c").join(address_df.alias("a"), col("c.address_id") == col("a.address_id"))
```

**Prevention**: Always use table aliases in complex joins with Spark 4.0.0

---

### **Issue 4: Foreign Key Constraint Violations**

**Problem**: Table refresh operations failing due to existing dependencies:
```
cannot drop table pagila_gold.gl_dim_staff because other objects depend on it
```

**Root Cause**: Dimensional tables have foreign key references from fact tables

**Solution**: Use proper dependency ordering and cascade operations
```python
# Drop fact tables first, then dimensions
DROP TABLE IF EXISTS pagila_gold.gl_fact_rental CASCADE;
DROP TABLE IF EXISTS pagila_gold.gl_dim_customer CASCADE;
```

**Prevention**: Design table refresh strategy considering dependency chains

---

### **Issue 5: Missing Silver Dependencies for Gold**

**Problem**: Gold tables empty even when silver transformation appears successful

**Root Cause**: Silver table creation failures prevent gold layer from accessing required data

**Solution**: Verify silver table creation before gold transformation
```python
# Check silver tables exist and have data
def validate_silver_availability(spark, db_config):
    for table in ['sl_customer', 'sl_film', 'sl_address', 'sl_store']:
        count = extract_silver_table(spark, db_config, table).count()
        if count == 0:
            raise Exception(f"Silver table {table} is empty")
```

**Prevention**: Implement silver data validation before gold processing

---

## 🔧 Spark 4.0.0 Specific Considerations

### **Enhanced Type Safety**
Spark 4.0.0 introduced stricter type checking that requires explicit casting:

```python
# Type-safe patterns for Spark 4.0.0
.withColumn("uuid_field", expr("uuid()").cast(StringType()))
.withColumn("null_field", lit(None).cast(StringType()))
.withColumn("date_calc", expr("cast(date_arithmetic as int)"))
```

### **ANSI SQL Mode Impact**
```python
# Configure for compatibility
.config("spark.sql.ansi.enabled", "true")  # Stricter type checking
.config("spark.sql.adaptive.enabled", "true")  # Performance optimization
```

---

## 📊 Data Pipeline Dependencies

### **Silver Layer Requirements**
- ✅ Bronze tables must exist with data
- ✅ All `lit(None)` values explicitly cast to target types
- ✅ UUID generation using `expr("uuid()")` for consistency
- ✅ Audit fields properly typed and populated

### **Gold Layer Requirements**
- ✅ Silver tables must exist and contain data
- ✅ Date dimension arithmetic properly cast to INT types
- ✅ Foreign key relationships maintained with UUID types
- ✅ Star schema integrity enforced

---

## 🔍 Diagnostic Queries

### **Verify Silver Table Population**
```sql
-- Check silver layer completeness
SELECT 
    'sl_customer' as table_name, COUNT(*) as records FROM pagila_silver.sl_customer
UNION ALL SELECT 'sl_film', COUNT(*) FROM pagila_silver.sl_film
UNION ALL SELECT 'sl_address', COUNT(*) FROM pagila_silver.sl_address
UNION ALL SELECT 'sl_store', COUNT(*) FROM pagila_silver.sl_store
UNION ALL SELECT 'sl_language', COUNT(*) FROM pagila_silver.sl_language;
```

### **Check Gold Dimensional Model**
```sql
-- Verify star schema integrity
SELECT 
    'gl_dim_customer' as table_name, COUNT(*) as records FROM pagila_gold.gl_dim_customer
UNION ALL SELECT 'gl_dim_film', COUNT(*) FROM pagila_gold.gl_dim_film
UNION ALL SELECT 'gl_fact_rental', COUNT(*) FROM pagila_gold.gl_fact_rental;
```

### **Data Quality Validation**
```sql
-- Check silver data quality scores
SELECT 
    AVG(sl_data_quality_score) as avg_quality,
    COUNT(CASE WHEN sl_data_quality_score >= 0.8 THEN 1 END) as high_quality_count,
    COUNT(*) as total_records
FROM pagila_silver.sl_customer;
```

---

## ⚠️ Warning Signs

### **Silver Layer Issues**
- JDBC void type errors in logs
- Silver tables showing 0 records despite bronze data
- Type casting failures in transformation logs
- Missing audit fields in silver tables

### **Gold Layer Issues**
- Date dimension generation failures
- Empty fact/dimension tables despite silver data
- Foreign key constraint violations
- BIGINT/INT type mismatch errors

---

## 🚀 Quick Fixes

### **Emergency JDBC Fix**
```python
# Quick fix for void type errors - add explicit casting
def emergency_type_cast_fix(df):
    return df.withColumn("fixed_field", 
                        when(col("problematic_field").isNull(), 
                             lit(None).cast(StringType()))
                        .otherwise(col("problematic_field")))
```

### **Date Dimension Fix**
```python
# Fix for date arithmetic type mismatch
def fix_date_dimension_types(df):
    return df.withColumn("safe_date_calc",
                        expr("date_add(base_date, cast(day_offset as int))"))
```

---

## 📈 Performance Benchmarks

### **Expected Performance**
- **Silver transformation**: 1,605 records processed in <5 minutes
- **Gold transformation**: Complete dimensional model in <10 minutes  
- **Data quality**: >80% of records with quality score >= 0.8

### **Success Criteria** 
- ✅ Silver tables: customer(599), film(1000), language(6), address(599), store(2) records
- ✅ Gold dimensions: All populated with proper foreign keys
- ✅ Fact tables: Proper measure aggregation and relationships
- ✅ No JDBC type errors in transformation logs

### **Pipeline Performance After Fixes**
- **DAG Success Rate**: 100% after memory and type fixes
- **Error-Free Execution**: 47.6 seconds end-to-end
- **Data Completeness**: All silver and gold tables populated

---

## 🔄 Recovery Procedures

### **Silver Layer Recovery**
```bash
# 1. Clear failed silver tables
psql -d silver_gold_dimensional -c "TRUNCATE pagila_silver.sl_address, pagila_silver.sl_store;"

# 2. Restart silver transformation with fixes
# 3. Verify record counts match expectations
```

### **Gold Layer Recovery**
```bash
# 1. Check silver dependencies first
# 2. Clear gold tables if needed
# 3. Restart gold transformation
# 4. Validate dimensional integrity
```

---

## 📚 Related Documentation

- **Transform Code**: `src/silver_gold_dimensional/transforms/bronze_to_silver_transforms.py`
- **DAG Definition**: `airflow/dags/silver_gold/silver_gold_dimensional_pipeline.py`
- **Architecture Guide**: `src/silver_gold_dimensional/transforms/CLAUDE.md`
- **Spark 4.0.0 Migration Guide**: [Apache Spark Documentation](https://spark.apache.org/docs/4.0.0/sql-migration-guide.html)

---

## 🎯 Common Patterns

### **Safe Type Casting Pattern**
```python
# Always use explicit casting for JDBC compatibility
def safe_column_creation(df, column_name, value, target_type):
    return df.withColumn(column_name, 
                        lit(value).cast(target_type) if value is not None 
                        else lit(None).cast(target_type))
```

### **Dimensional Key Pattern**
```python
# Consistent UUID generation for dimensional keys
def add_dimensional_key(df, key_name):
    return df.withColumn(key_name, expr("uuid()").cast(StringType()))
```

### **Date Dimension Pattern**
```python
# Safe date arithmetic for Spark 4.0.0
def safe_date_calculation(df, base_date, offset_column):
    return df.withColumn("calculated_date",
                        expr(f"date_add('{base_date}', cast({offset_column} as int))"))
```

---

**🔑 Key Takeaway**: Silver-Gold dimensional modeling requires careful attention to Spark 4.0.0 type safety, proper JDBC type handling, and maintaining data pipeline dependencies. Always validate data flow between layers and implement proper error handling.**
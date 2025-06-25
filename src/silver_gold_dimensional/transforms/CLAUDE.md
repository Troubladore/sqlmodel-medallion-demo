# 🥈🥇 Silver-Gold Dimensional Methodology - Standards & Implementation Guide

> **Purpose**: Document the silver-gold dimensional modeling methodology, standards, and best practices for implementing classic data warehouse patterns in the medallion architecture.  
> **Audience**: Data engineers implementing dimensional modeling, business intelligence, and data warehouse solutions  
> **Scope**: Complete silver-gold dimensional pipeline using Spark 4.0.0 + PySpark 4.0.0 with star schema design

---

## 🎯 Silver-Gold Dimensional Philosophy

**Silver-Gold Dimensional** implements the **classic data warehouse dimensional modeling** pattern optimized for:
- Business intelligence and analytics workloads
- Star schema dimensional design (facts + dimensions)
- Pre-calculated business metrics and KPIs
- Slowly changing dimensions (SCD) management
- Denormalized structures for query performance

**Key Principles**:
1. **Silver Layer**: Clean, business-ready data with quality scores
2. **Gold Layer**: Star schema with facts and conformed dimensions
3. **Business Logic**: Pre-calculated metrics and segmentation
4. **Denormalization**: Optimize for analytical query performance
5. **Audit Consistency**: Complete lineage from bronze through gold

---

## 🏗️ Two-Layer Architecture Pattern

### **Silver Layer Strategy**
**Purpose**: Data cleansing, standardization, and business rule application

```python
# Silver layer transformation pattern
def transform_customer_to_silver(spark: SparkSession, db_config: dict, batch_id: str):
    """Clean bronze data into business-ready silver entities"""
    
    # 1. Extract bronze data with lenient types
    bronze_df = extract_bronze_table(spark, db_config, "br_customer")
    
    # 2. Clean and standardize with business rules
    clean_df = (bronze_df
        .withColumn("sl_customer_key", expr("uuid()"))  # Silver surrogate key
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("customer_id", col("customer_id").cast(IntegerType())))
    
    # 3. Calculate data quality scores
    quality_df = calculate_data_quality_score(clean_df, required_fields)
    
    # 4. Add silver audit fields
    silver_df = add_silver_audit_fields(quality_df, "customer", batch_id)
    
    # 5. Load with full refresh
    load_to_silver_table(silver_df, db_config, "sl_customer")
```

### **Gold Layer Strategy** 
**Purpose**: Star schema dimensional modeling with facts and dimensions

```python
# Gold layer dimensional pattern
def transform_customer_dimension(spark: SparkSession, db_config: dict, batch_id: str):
    """Create conformed customer dimension with analytics"""
    
    # 1. Extract clean silver data
    silver_customer = extract_silver_table(spark, db_config, "sl_customer")
    
    # 2. Join with related dimensions for denormalization
    customer_with_address = silver_customer.join(silver_address, "address_id", "left")
    
    # 3. Calculate business metrics from transactional data
    customer_analytics = calculate_customer_metrics(bronze_rental, bronze_payment)
    
    # 4. Build comprehensive dimension with segmentation
    customer_dim = (customer_with_address
        .join(customer_analytics, "customer_id", "left")
        .withColumn("gl_dim_customer_key", expr("uuid()"))
        .withColumn("customer_tier", calculate_customer_tier())
        .withColumn("is_high_value", determine_high_value_status()))
    
    # 5. Load to gold dimension
    load_to_gold_table(customer_dim, db_config, "gl_dim_customer")
```

---

## 📊 Star Schema Design Patterns

### **Dimensional Model Structure**

**Facts (Measures and Metrics)**:
- `gl_fact_rental`: Central fact table with rental transactions
- Additive measures: revenue_amount, rental_rate, replacement_cost
- Semi-additive measures: days_overdue, rental_duration_actual
- Calculated measures: is_overdue, is_returned, is_weekend_rental

**Dimensions (Attributes and Hierarchies)**:
- `gl_dim_customer`: Customer attributes with denormalized address and analytics
- `gl_dim_film`: Film attributes with categories and revenue tiers
- `gl_dim_store`: Store attributes with location information
- `gl_dim_staff`: Staff attributes and organizational hierarchy
- `gl_dim_date`: Comprehensive calendar dimension with business attributes

### **Fact Table Design Pattern**

```python
class GlFactRental(SQLModel, table=True):
    __tablename__ = "gl_fact_rental"
    __table_args__ = {"schema": "pagila_gold"}

    # Gold surrogate key
    gl_fact_rental_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    rental_id: int = Field(nullable=False, unique=True)
    
    # Dimension foreign keys (star schema)
    customer_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_customer.gl_dim_customer_key")))
    film_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_film.gl_dim_film_key")))
    store_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_store.gl_dim_store_key")))
    staff_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_staff.gl_dim_staff_key")))
    rental_date_key: int = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_date.date_key")))
    
    # Additive measures
    rental_rate: float = Field(sa_column=Column(Numeric(4, 2), nullable=False))
    replacement_cost: float = Field(sa_column=Column(Numeric(5, 2), nullable=False))
    revenue_amount: Optional[float] = Field(sa_column=Column(Numeric(5, 2), nullable=True))
    
    # Calculated measures
    rental_duration_actual: Optional[int] = Field(default=None, nullable=True)
    is_overdue: bool = Field(default=False, nullable=False)
    days_overdue: Optional[int] = Field(default=None, nullable=True)
    
    # Analytics attributes
    is_weekend_rental: bool = Field(default=False, nullable=False)
    rental_quarter: int = Field(nullable=False)
```

### **Dimension Table Design Pattern**

```python
class GlDimCustomer(SQLModel, table=True):
    __tablename__ = "gl_dim_customer"
    __table_args__ = {"schema": "pagila_gold"}

    # Gold surrogate key
    gl_dim_customer_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    customer_id: int = Field(nullable=False, unique=True)
    
    # Core customer attributes
    first_name: str = Field(nullable=False, max_length=45)
    last_name: str = Field(nullable=False, max_length=45)
    full_name: str = Field(nullable=False, max_length=91)
    
    # Denormalized address attributes (SCD Type 1)
    address_line1: str = Field(nullable=False, max_length=50)
    city: str = Field(nullable=False, max_length=50)
    country: str = Field(nullable=False, max_length=50)
    
    # Pre-calculated analytics (business metrics)
    total_rentals: int = Field(default=0, nullable=False)
    total_payments: float = Field(sa_column=Column(Numeric(8, 2), default=0.00))
    customer_lifetime_value: Optional[float] = Field(sa_column=Column(Numeric(8, 2)))
    
    # Customer segmentation
    customer_tier: Optional[str] = Field(max_length=20)  # Bronze, Silver, Gold, Platinum
    is_high_value: bool = Field(default=False, nullable=False)
    preferred_genre: Optional[str] = Field(max_length=25)
```

---

## 🔄 Transformation Implementation Patterns

### **1. Data Quality and Cleansing (Silver)**

```python
def calculate_data_quality_score(df: DataFrame, required_fields: List[str]) -> DataFrame:
    """Calculate comprehensive data quality score"""
    
    quality_score = lit(1.0)  # Start with perfect score
    
    for field in required_fields:
        if field in df.columns:
            # Deduct points for nulls and empty strings
            quality_score = quality_score - when(
                (col(field).isNull()) | (trim(col(field)) == ""), 
                lit(0.2)
            ).otherwise(lit(0.0))
    
    # Ensure score stays between 0 and 1
    quality_score = when(quality_score < lit(0.0), lit(0.0)).otherwise(quality_score)
    
    return df.withColumn("sl_data_quality_score", quality_score)
```

### **2. Business Metrics Calculation (Gold)**

```python
def calculate_customer_analytics(bronze_rental: DataFrame, bronze_payment: DataFrame) -> DataFrame:
    """Calculate comprehensive customer business metrics"""
    
    # Rental analytics
    rental_analytics = (bronze_rental
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .withColumn("rental_date", to_date(col("rental_date")))
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_rentals"),
            spark_min("rental_date").alias("first_rental_date"),
            spark_max("rental_date").alias("last_rental_date")
        ))
    
    # Payment analytics
    payment_analytics = (bronze_payment
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .withColumn("amount", col("amount").cast(FloatType()))
        .groupBy("customer_id")
        .agg(
            spark_sum("amount").alias("total_payments"),
            avg("amount").alias("avg_payment_amount")
        ))
    
    # Combine analytics
    return rental_analytics.join(payment_analytics, "customer_id", "full_outer")
```

### **3. Customer Segmentation (Gold)**

```python
def apply_customer_segmentation(df: DataFrame) -> DataFrame:
    """Apply business rules for customer segmentation"""
    
    return (df
        .withColumn("customer_lifetime_value", col("total_payments"))
        .withColumn("customer_tier",
            when(col("total_payments") >= lit(100.0), "Platinum")
            .when(col("total_payments") >= lit(50.0), "Gold")
            .when(col("total_payments") >= lit(20.0), "Silver")
            .otherwise("Bronze"))
        .withColumn("is_high_value", 
            when(col("total_payments") >= lit(50.0), True).otherwise(False))
        .withColumn("avg_rental_frequency", 
            coalesce(col("total_rentals") / 12.0, lit(0.0))))  # Monthly frequency
```

### **4. Date Dimension Generation**

```python
def generate_date_dimension(spark: SparkSession) -> DataFrame:
    """Generate comprehensive date dimension for time intelligence"""
    
    # Create date range (2000-2030)
    start_date = date(2000, 1, 1)
    end_date = date(2030, 12, 31)
    
    # Generate sequence of dates with comprehensive attributes
    date_df = spark.range(0, (end_date - start_date).days + 1).select(
        (lit(start_date.toordinal()) + col("id")).alias("date_ordinal")
    ).withColumn(
        "calendar_date", 
        expr("date_add('1900-01-01', date_ordinal - 693594)")
    )
    
    # Add business calendar attributes
    return (date_df
        .withColumn("date_key", 
            (year("calendar_date") * 10000 + 
             month("calendar_date") * 100 + 
             dayofmonth("calendar_date")).cast(IntegerType()))
        .withColumn("fiscal_year", 
            when(col("calendar_month") >= 4, col("calendar_year"))
            .otherwise(col("calendar_year") - 1))
        .withColumn("is_weekend", 
            when(col("day_of_week").isin([1, 7]), True).otherwise(False))
        .withColumn("is_holiday", lit(False)))  # Business logic placeholder
```

---

## 📋 Implementation Standards

### **1. Silver Layer Standards**

**Data Cleansing Rules**:
- Cast all bronze TEXT fields to appropriate business types
- Trim whitespace and standardize case
- Apply null handling and default values
- Calculate comprehensive data quality scores

**Audit Fields Pattern**:
```python
# Required silver audit fields
sl_created_time: datetime      # When silver record was created
sl_updated_time: datetime      # When silver record was last updated
sl_source_bronze_key: UUID     # Link back to bronze source
sl_is_active: bool            # Record active status
sl_data_quality_score: float  # Calculated quality score (0.0-1.0)
```

**Business Logic Application**:
- Email standardization (lowercase, trimming)
- Name formatting and full name concatenation
- Address cleaning and standardization
- Business rule validation

### **2. Gold Layer Standards**

**Dimensional Design Rules**:
- All dimensions have surrogate keys (UUID)
- Preserve natural business keys
- Denormalize for query performance
- Pre-calculate frequently used metrics

**Fact Table Rules**:
- Central fact table with foreign keys to all dimensions
- Additive measures for aggregation
- Calculated measures for business logic
- Date/time dimensions for time intelligence

**Star Schema Implementation**:
```python
# Fact table dimension foreign keys
customer_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_customer.gl_dim_customer_key")))
film_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_film.gl_dim_film_key")))
store_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_store.gl_dim_store_key")))
staff_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_staff.gl_dim_staff_key")))
rental_date_key: int = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_date.date_key")))
```

### **3. Performance Optimization Standards**

**Spark 4.0.0 Dimensional Settings**:
```python
# Enhanced settings for dimensional processing
.config("spark.sql.adaptive.autoBroadcastJoinThreshold", "50MB")  # Broadcast small dimensions
.config("spark.sql.adaptive.localShuffleReader.enabled", "true")  # Optimize joins
.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")  # Partition sizing
.config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "20MB")  # Coalescing
```

**Join Optimization**:
- Broadcast small dimension tables
- Use bucketing for large fact tables
- Optimize partition strategies for time-based queries

---

## 📊 Business Intelligence Integration

### **Dimensional Model Validation**

```python
# Comprehensive dimensional model validation
dimensional_validations = [
    {
        "name": "Silver Layer Completeness",
        "query": "SELECT COUNT(*) FROM pagila_silver.sl_customer WHERE sl_data_quality_score >= 0.8",
        "rule": "At least 80% of silver records should have high quality scores"
    },
    {
        "name": "Star Schema Integrity", 
        "query": """
            SELECT COUNT(*) as fact_count,
                   COUNT(DISTINCT customer_key) as unique_customers,
                   COUNT(DISTINCT film_key) as unique_films
            FROM pagila_gold.gl_fact_rental
        """,
        "rule": "Fact table should have proper foreign key distribution"
    },
    {
        "name": "Customer Segmentation",
        "query": """
            SELECT customer_tier, COUNT(*) as customer_count
            FROM pagila_gold.gl_dim_customer
            GROUP BY customer_tier
        """,
        "rule": "Customer segmentation should be properly distributed"
    }
]
```

### **BI Readiness Metrics**:
- Data freshness (last batch completion)
- Quality scores (>80% high quality)
- Dimensional integrity (valid foreign keys)
- Metric completeness (calculated fields populated)

---

## 🚨 Common Issues & Solutions

### **Issue: Silver Data Quality Degradation**
**Root Cause**: Bronze data quality issues or transformation logic errors  
**Solution**: Implement comprehensive quality scoring and alerting  
**Prevention**: Validate bronze data before silver transformation

### **Issue: Gold Dimension Explosion**
**Root Cause**: Inappropriate denormalization or join logic  
**Solution**: Careful dimension design with proper deduplication  
**Prevention**: Test dimension cardinality and validate business rules

### **Issue: Fact Table Performance** 
**Root Cause**: Large fact tables without proper partitioning  
**Solution**: Implement date-based partitioning and indexing  
**Prevention**: Design with query patterns in mind

### **Issue: Slowly Changing Dimension Complexity**
**Root Cause**: Attempting complex SCD types without proper framework  
**Solution**: Start with SCD Type 1, evolve to Type 2 when needed  
**Prevention**: Document SCD strategy clearly

---

## 📚 Usage Examples

### **Complete Pipeline Execution**:
```python
# Initialize Spark with dimensional optimization
spark = (SparkSession.builder
         .appName("Dimensional_Modeling_Pipeline")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "50MB")
         .getOrCreate())

# Database configuration
db_config = {
    'host': 'host.docker.internal',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres'
}

# Execute silver transformation
silver_results = bronze_to_silver_transforms.run(spark, db_config, execution_date)

# Execute gold transformation  
gold_results = silver_to_gold_dimensional.run(spark, db_config, execution_date)

print(f"Silver processed: {silver_results['total_silver_rows_processed']} rows")
print(f"Gold processed: {gold_results['total_gold_rows_processed']} rows")
```

### **Business Intelligence Queries**:
```sql
-- Customer analytics query
SELECT 
    c.customer_tier,
    COUNT(*) as customer_count,
    AVG(c.total_rentals) as avg_rentals,
    SUM(f.revenue_amount) as total_revenue
FROM pagila_gold.gl_dim_customer c
JOIN pagila_gold.gl_fact_rental f ON c.gl_dim_customer_key = f.customer_key
GROUP BY c.customer_tier
ORDER BY total_revenue DESC;

-- Time intelligence query
SELECT 
    d.calendar_year,
    d.calendar_quarter,
    COUNT(f.rental_id) as total_rentals,
    SUM(f.revenue_amount) as quarterly_revenue
FROM pagila_gold.gl_fact_rental f
JOIN pagila_gold.gl_dim_date d ON f.rental_date_key = d.date_key
GROUP BY d.calendar_year, d.calendar_quarter
ORDER BY d.calendar_year, d.calendar_quarter;
```

---

## 🔄 Deployment & Scheduling

### **Airflow DAG Configuration**:
```python
# Dimensional modeling DAG
DAG_ID = 'silver_gold_dimensional_pipeline'
SCHEDULE_INTERVAL = '@daily'  # Daily refresh for consistency
MAX_ACTIVE_RUNS = 1  # Prevent overlapping dimensional refreshes

# Task dependencies
start >> preflight_checks >> run_silver >> run_gold >> validate_model >> notify >> end
```

### **Processing Order**:
1. **Preflight Checks**: Validate bronze availability and target readiness
2. **Silver Transform**: Clean and standardize bronze data
3. **Gold Dimensions**: Build conformed dimension tables
4. **Gold Facts**: Create fact tables with measures
5. **Validation**: Verify dimensional model integrity
6. **Notification**: Alert BI team of readiness

---

## 🎯 Success Criteria

**Implementation Complete When**:
- [ ] Silver layer achieves >80% data quality scores
- [ ] Star schema maintains referential integrity
- [ ] Fact tables have proper measure aggregation
- [ ] Dimensions support business analytics requirements
- [ ] Query performance meets BI SLA requirements
- [ ] Complete audit trail from bronze to gold

**Performance Benchmarks**:
- Silver transformation: <10 minutes for 35K+ bronze records
- Gold transformation: <15 minutes for complete dimensional refresh
- Query performance: <5 seconds for standard BI queries
- Data freshness: Daily updates with <24 hour latency

---

## 📚 Critical Production Issues & Solutions

### **Lessons Learned Reference**
For comprehensive troubleshooting and resolution of silver-gold pipeline issues, see [FAQ.md](../FAQ.md):

- **JDBC Void Type Errors**: Critical fix for `lit(None)` type casting issues
- **Spark 4.0.0 Compatibility**: ANSI SQL mode and data type handling
- **Memory Configuration**: Container and Spark memory allocation
- **Pipeline Dependencies**: Silver table creation and gold layer dependencies

### **Critical Fixes Applied**
1. **JDBC Type Safety**: All `lit(None)` values must be cast to explicit types (StringType, IntegerType, etc.)
2. **Memory Optimization**: Spark driver/executor memory increased to 512MB minimum for Spark 4.0.0
3. **Type Casting**: UUID generation and literal values require explicit type casting for JDBC compatibility
4. **Pipeline Validation**: Silver table completeness validation before gold transformation

### **Production Success Metrics**
- **DAG Success Rate**: 100% after fixes
- **End-to-End Time**: 47.6 seconds (bronze → silver → gold)
- **Data Completeness**: 1,605 silver records → populated gold dimensions and facts
- **Error Elimination**: Zero JDBC type errors, no memory failures

---

**🚀 Next Steps**: Use this dimensional methodology for business intelligence, reporting, and analytics. Extend with slowly changing dimensions (SCD Type 2) and additional fact tables as business requirements evolve.
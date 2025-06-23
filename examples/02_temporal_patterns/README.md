# Temporal Patterns - Time Distinction

## 🎯 Core Principle

**Clear distinction between business time and system audit time.**

## ⏰ Time Field Types

### `systime` - System Audit Time
**When the record was created/modified in the database system**

```python
systime: datetime = Field(
    default_factory=lambda: datetime.now(UTC),
    sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
)
```

### `effective_time` - Business Effective Time  
**When the data is/was effective for business purposes**

```python
effective_time: datetime = Field(
    default_factory=lambda: datetime.now(UTC),
    sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
)
```

## 🏗️ Complete Examples

### Business Entity with Both Time Types

```python
# src/pagila/tables/pagila/customer.py
import uuid
from datetime import datetime, UTC
from typing import Optional
from uuid import UUID

from sqlalchemy import TIMESTAMP, Boolean, Column, text
from sqlmodel import Field, SQLModel

class Customer(SQLModel, table=True):
    __tablename__ = "customer"
    __table_args__ = {"schema": "public"}

    # Business entity - UUID primary key
    customer_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    first_name: str = Field(nullable=False)
    last_name: str = Field(nullable=False)
    email: Optional[str] = Field(default=None)
    active: bool = Field(default=True)
    
    # ✅ Business effective time - when customer data became effective
    effective_time: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    
    # ✅ System audit time - when record was last modified in database
    systime: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
```

### Reference Table with System Time Only

```python
# src/pagila/tables/pagila/category.py
from datetime import datetime, UTC
from typing import Optional

from sqlalchemy import SMALLINT, TIMESTAMP, Column
from sqlmodel import Field, SQLModel

class Category(SQLModel, table=True):
    __tablename__ = "category"
    __table_args__ = {"schema": "public"}

    category_id: int = Field(primary_key=True, sa_type=SMALLINT)
    name: str = Field(nullable=False, max_length=25)
    
    # Soft delete timestamp
    inactivated_date: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=True)
    )
    
    # ✅ System audit time only (reference data doesn't need business effective time)
    systime: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
```

### Temporal History Table

```python
# src/pagila/tables/pagila/customer_history.py
from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import TIMESTAMP, Boolean, Column, text
from sqlmodel import Field, SQLModel

class CustomerHistory(SQLModel, table=True):
    __tablename__ = "customer_history"
    __table_args__ = {"schema": "public"}

    # Composite primary key: business key + version timestamp
    customer_id: UUID = Field(primary_key=True)
    sys_start: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            primary_key=True,
            default=text("clock_timestamp()")
        )
    )
    sys_end: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            default=text("'infinity'")
        )
    )
    
    # Business data fields
    first_name: str = Field(nullable=False)
    last_name: str = Field(nullable=False)  
    email: Optional[str] = Field(default=None)
    active: bool = Field(default=True)
    
    # ✅ Business effective time - preserved from original record
    effective_time: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    
    # ✅ System time when this version was created
    systime: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
```

## 📊 Usage Scenarios

### Business Effective Time Examples

```python
# Customer data effective from a future date
customer = Customer(
    first_name="John",
    last_name="Doe", 
    email="john@example.com",
    effective_time=datetime(2024, 1, 1, tzinfo=UTC),  # Effective from Jan 1, 2024
    systime=datetime.now(UTC)  # Created now in system
)

# Historical correction - data effective from past date
customer = Customer(
    first_name="Jane", 
    last_name="Smith",
    email="jane@example.com",
    effective_time=datetime(2023, 6, 15, tzinfo=UTC),  # Was effective from June 15, 2023
    systime=datetime.now(UTC)  # But entered into system today
)
```

### System Time Queries

```python
# Find all records modified in the last hour
recent_updates = session.exec(
    select(Customer).where(
        Customer.systime >= datetime.now(UTC) - timedelta(hours=1)
    )
).all()

# Audit trail - what changed when
audit_trail = session.exec(
    select(Customer.customer_id, Customer.systime, Customer.effective_time)
    .order_by(Customer.systime.desc())
).all()
```

### Temporal Queries (History Tables)

```python
# Point-in-time query - customer state as of specific date
customer_at_date = session.exec(
    select(CustomerHistory)
    .where(CustomerHistory.customer_id == customer_id)
    .where(CustomerHistory.sys_start <= target_date)
    .where(CustomerHistory.sys_end > target_date)
).first()

# Version history for a customer
version_history = session.exec(
    select(CustomerHistory)
    .where(CustomerHistory.customer_id == customer_id)
    .order_by(CustomerHistory.sys_start)
).all()
```

## 🎯 Decision Matrix

| Table Type | systime | effective_time | Use Case |
|------------|---------|----------------|----------|
| **Reference Data** | ✅ Required | ❌ Not needed | Categories, Languages, Static lookups |
| **Business Entities** | ✅ Required | ✅ Required | Customers, Films, Rentals |
| **Junction Tables** | ✅ Required | ✅ Optional* | FilmActor, FilmCategory |
| **History Tables** | ✅ Required | ✅ Preserved | CustomerHistory, FilmHistory |

*Junction tables need `effective_time` if the relationship itself has business meaning over time.

## ❌ Common Mistakes

### DON'T: Confuse the two time types
```python
# ❌ Wrong - using systime for business logic
rental_due_date = customer.systime + timedelta(days=7)

# ✅ Right - using effective_time for business logic  
rental_due_date = customer.effective_time + timedelta(days=7)
```

### DON'T: Mix naming conventions
```python
# ❌ Wrong - inconsistent field names
created_at: datetime = Field(...)
last_modified: datetime = Field(...)

# ✅ Right - consistent with proven patterns
effective_time: datetime = Field(...)
systime: datetime = Field(...)
```

### DON'T: Forget timezone awareness
```python
# ❌ Wrong - naive datetime
systime: datetime = Field(default_factory=datetime.now)

# ✅ Right - UTC timezone
systime: datetime = Field(default_factory=lambda: datetime.now(UTC))
```

## 🔍 Why This Distinction Matters

1. **Business Logic** uses `effective_time` for calculations and rules
2. **Audit Trails** use `systime` for tracking system changes
3. **Data Corrections** can have past `effective_time` but current `systime`
4. **Future Events** can have future `effective_time` but current `systime`
5. **Historical Analysis** needs both time dimensions for complete picture

This temporal pattern enables sophisticated time-travel queries, audit trails, and business rule enforcement while maintaining clear separation of concerns.
# Table Type Mixins - Consistent Base Patterns

## 🎯 Core Principle

**Use mixins to enforce consistent patterns across different table types.**

## 🏗️ Base Mixin Classes

### ReferenceTableMixin

```python
# src/_base/tables/table_type_mixins.py
from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import TIMESTAMP, Column
from sqlmodel import Field

class ReferenceTableMixin:
    """
    Mixin for reference/lookup tables.
    
    Adds standard fields for reference data:
    - inactivated_date: UTC datetime when record was inactivated (NULL = active)
    - systime: audit timestamp for when record was created/last modified
    
    Reference tables should use SMALLINT primary keys for performance.
    """
    
    inactivated_date: Optional[datetime] = Field(
        default=None, 
        sa_column=Column(TIMESTAMP(timezone=True), nullable=True)
    )
    systime: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
```

### TransactionalTableMixin

```python
class TransactionalTableMixin:
    """
    Mixin for transactional/business entity tables.
    
    Adds standard audit fields:
    - effective_time: business effective timestamp
    - systime: system audit timestamp (optional, can be overridden)
    
    Transactional tables typically use UUID primary keys.
    """
    
    effective_time: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
```

### TemporalTableMixin

```python
class TemporalTableMixin:
    """
    Mixin for temporal/history tables.
    
    Adds standard temporal fields for system versioning:
    - sys_start: system-time start (when record became current)
    - sys_end: system-time end (when record was superseded, 'infinity' for current)
    
    Used with PostgreSQL temporal extensions or manual history tracking.
    """
    
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
```

## 📋 Abstract Base Classes

### ReferenceTableBase

```python
from sqlmodel import SQLModel

class ReferenceTableBase(ReferenceTableMixin, SQLModel, table=True):
    """
    Base class for reference/lookup tables.
    
    Provides:
    - SMALLINT primary key (override field name in subclass)
    - inactivated_date for temporal soft deletes (NULL = active)
    - systime for audit trail
    """
    
    # This is abstract - subclasses must define their own primary key
    __abstract__ = True
```

### TransactionalTableBase  

```python
class TransactionalTableBase(TransactionalTableMixin, SQLModel, table=True):
    """
    Base class for transactional/business entity tables.
    
    Provides:
    - UUID primary key (override field name in subclass)
    - effective_time for business temporal tracking
    """
    
    # This is abstract - subclasses must define their own primary key
    __abstract__ = True
```

### TemporalTableBase

```python
class TemporalTableBase(TemporalTableMixin, SQLModel, table=True):
    """
    Base class for temporal/history tables.
    
    Provides:
    - sys_start/sys_end for system versioning
    - Composite primary key with business key + sys_start
    """
    
    # This is abstract - subclasses must define their own business primary key
    __abstract__ = True
```

## 🎯 Pagila Implementation Examples

### Reference Table Using Mixin

```python
# src/pagila/tables/pagila/category.py
from typing import List, TYPE_CHECKING

from sqlalchemy import SMALLINT
from sqlmodel import Field, Relationship, SQLModel

from src._base.tables.table_type_mixins import ReferenceTableMixin

if TYPE_CHECKING:
    from .film_category import FilmCategory

class Category(ReferenceTableMixin, SQLModel, table=True):
    __tablename__ = "category"
    __table_args__ = {"schema": "public"}

    # ✅ SMALLINT primary key for reference table
    category_id: int = Field(primary_key=True, sa_type=SMALLINT)
    name: str = Field(nullable=False, max_length=25)
    description: Optional[str] = Field(default=None, max_length=500)
    
    # ✅ inactivated_date and systime inherited from ReferenceTableMixin
    
    # Relationships
    film_categories: List["FilmCategory"] = Relationship(back_populates="category")
```

### Business Entity Using Base Class

```python
# src/pagila/tables/pagila/customer.py
import uuid
from typing import Optional, List, TYPE_CHECKING
from uuid import UUID

from sqlmodel import Field, Relationship

from src._base.tables.table_type_mixins import TransactionalTableBase

if TYPE_CHECKING:
    from .rental import Rental
    from .payment import Payment

class Customer(TransactionalTableBase):
    __tablename__ = "customer"
    __table_args__ = {"schema": "public"}

    # ✅ UUID primary key for business entity
    customer_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    first_name: str = Field(nullable=False)
    last_name: str = Field(nullable=False)
    email: Optional[str] = Field(default=None)
    active: bool = Field(default=True)
    
    # ✅ effective_time inherited from TransactionalTableBase
    
    # Relationships
    rentals: List["Rental"] = Relationship(back_populates="customer")
    payments: List["Payment"] = Relationship(back_populates="customer")
```

### Temporal Table Using Base Class

```python
# src/pagila/tables/pagila/customer_history.py
from typing import Optional
from uuid import UUID

from sqlmodel import Field

from src._base.tables.table_type_mixins import TemporalTableBase

class CustomerHistory(TemporalTableBase):
    __tablename__ = "customer_history"
    __table_args__ = {"schema": "public"}

    # ✅ Business primary key + sys_start from TemporalTableBase
    customer_id: UUID = Field(primary_key=True)
    
    # Business data fields (preserved from main table)
    first_name: str = Field(nullable=False)
    last_name: str = Field(nullable=False)
    email: Optional[str] = Field(default=None)
    active: bool = Field(default=True)
    
    # ✅ sys_start and sys_end inherited from TemporalTableBase
```

## 🔍 Table Type Detection

```python
def get_table_type(table_class: type) -> str:
    """
    Classify a table by its inheritance hierarchy and naming conventions.
    
    Returns:
        str: One of "reference", "transactional", "temporal", "unknown"
    """
    
    # Check inheritance hierarchy first (most reliable)
    if issubclass(table_class, ReferenceTableBase):
        return "reference"
    elif issubclass(table_class, TransactionalTableBase):
        return "transactional"
    elif issubclass(table_class, TemporalTableBase):
        return "temporal"
    
    # Fallback to mixin detection
    if issubclass(table_class, ReferenceTableMixin):
        return "reference"
    elif issubclass(table_class, TransactionalTableMixin):
        return "transactional"
    elif issubclass(table_class, TemporalTableMixin):
        return "temporal"
    
    # Fallback to naming conventions
    table_name = getattr(table_class, "__tablename__", "")
    if table_name.endswith("_type") or table_name.endswith("_status_type"):
        return "reference"
    elif table_name.endswith("_hist"):
        return "temporal"
    
    return "unknown"

# Usage examples:
print(get_table_type(Category))        # "reference"
print(get_table_type(Customer))        # "transactional"  
print(get_table_type(CustomerHistory)) # "temporal"
```

## 📊 Mixin vs Base Class Decision Matrix

| Scenario | Use Mixin | Use Base Class | Example |
|----------|-----------|----------------|---------|
| **Standard pattern** | ❌ | ✅ | Most reference tables |
| **Custom fields needed** | ✅ | ❌ | Reference table with extra audit fields |
| **Multiple inheritance** | ✅ | ❌ | Table needs multiple mixins |
| **Abstract base desired** | ❌ | ✅ | Enforce primary key definition |

## ❌ Common Pitfalls

### DON'T: Forget to define primary key in abstract base
```python
# ❌ Wrong - missing primary key in concrete class
class Category(ReferenceTableBase):
    __tablename__ = "category"
    name: str = Field(nullable=False)
    # Missing: category_id primary key field
```

### DON'T: Duplicate mixin fields
```python
# ❌ Wrong - duplicating mixin fields
class Category(ReferenceTableMixin, SQLModel, table=True):
    category_id: int = Field(primary_key=True)
    name: str = Field(nullable=False)
    systime: datetime = Field(...)  # Already in mixin!
```

### DON'T: Mix incompatible mixins
```python
# ❌ Wrong - reference tables don't need effective_time
class Category(ReferenceTableMixin, TransactionalTableMixin, SQLModel, table=True):
    # This creates confusing dual time semantics
```

## ✅ Migration Strategy

1. **Identify** existing table patterns in your codebase
2. **Classify** tables as reference, transactional, or temporal
3. **Choose** mixin vs base class based on customization needs
4. **Inherit** from appropriate mixin/base class
5. **Remove** duplicate field definitions
6. **Validate** with `get_table_type()` function

## 🎯 Key Benefits

- ✅ **Consistent patterns** across all table types
- ✅ **Reduced duplication** of common fields
- ✅ **Automatic classification** for build systems
- ✅ **Type safety** with proper inheritance
- ✅ **Easy validation** with detection functions
- ✅ **Flexible customization** via mixins
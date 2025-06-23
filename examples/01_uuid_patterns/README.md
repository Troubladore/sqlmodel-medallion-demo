# UUID and Primary Key Patterns

## 🎯 Core Principle

**Business entities use UUID, reference/lookup tables use SMALLINT integers.**

## ✅ Proven Patterns

### Business Entity (UUID Primary Key)

```python
import uuid
from uuid import UUID
from sqlmodel import Field, SQLModel

class Film(SQLModel, table=True):
    __tablename__ = "film"
    __table_args__ = {"schema": "public"}
    
    # ✅ UUID for business entity
    film_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    title: str = Field(nullable=False)
    # ... other fields
```

### Reference Table (SMALLINT Primary Key)

```python
from sqlalchemy import SMALLINT
from sqlmodel import Field, SQLModel

class Category(SQLModel, table=True):
    __tablename__ = "category"
    __table_args__ = {"schema": "public"}
    
    # ✅ SMALLINT for reference table
    category_id: int = Field(primary_key=True, sa_type=SMALLINT)
    name: str = Field(nullable=False, max_length=25)
    # ... other fields
```

## 🏗️ Complete Examples

### Film Table (Business Entity)
```python
import uuid
from datetime import datetime, UTC
from typing import Optional, List, TYPE_CHECKING
from uuid import UUID

from sqlalchemy import TIMESTAMP, SMALLINT, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .category import Category
    from .language import Language

class Film(SQLModel, table=True):
    __tablename__ = "film"
    __table_args__ = {"schema": "public"}

    # Business entity - UUID primary key
    film_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    title: str = Field(nullable=False, max_length=255)
    description: Optional[str] = Field(default=None)
    release_year: Optional[int] = Field(default=None)
    
    # Foreign keys to reference tables (SMALLINT)
    language_id: int = Field(
        sa_column=Column(ForeignKey("public.language.language_id"), nullable=False),
        sa_type=SMALLINT
    )
    
    # Audit timestamp (system time)
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    
    # Relationships
    language: Optional["Language"] = Relationship(back_populates="films")
    categories: List["Category"] = Relationship(
        back_populates="films", 
        link_table="film_category"
    )
```

### Category Table (Reference Entity)
```python
from datetime import datetime, UTC
from typing import Optional, List, TYPE_CHECKING

from sqlalchemy import SMALLINT, TIMESTAMP, Column
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .film import Film

class Category(SQLModel, table=True):
    __tablename__ = "category"
    __table_args__ = {"schema": "public"}

    # Reference table - SMALLINT primary key
    category_id: int = Field(primary_key=True, sa_type=SMALLINT)
    name: str = Field(nullable=False, max_length=25)
    
    # Soft delete support (best practice for reference data)
    inactivated_date: Optional[datetime] = Field(
        default=None, 
        sa_column=Column(TIMESTAMP(timezone=True), nullable=True)
    )
    
    # System audit time
    systime: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    
    # Relationships
    films: List["Film"] = Relationship(
        back_populates="categories", 
        link_table="film_category"
    )
```

## 🎯 Key Decision Matrix

| Entity Type | Primary Key | Use Case | Example |
|-------------|-------------|----------|---------|
| **Business Entity** | `UUID` | Core business objects with real-world meaning | Film, Customer, Rental, Staff |
| **Reference/Lookup** | `SMALLINT` | Static lookup data, performance-critical | Category, Language, Country, City |
| **Junction Tables** | `Composite` | Many-to-many relationships | FilmCategory, FilmActor |
| **Temporal/History** | `UUID + timestamp` | Version tracking | FilmHistory, CustomerHistory |

## ❌ Common Mistakes

### DON'T: Mix UUID and Integer styles
```python
# ❌ Wrong - inconsistent primary key strategy
class Film(SQLModel, table=True):
    film_id: int = Field(primary_key=True)  # Should be UUID
    
class Category(SQLModel, table=True):
    category_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)  # Should be SMALLINT
```

### DON'T: Forget sa_type for foreign keys
```python
# ❌ Wrong - foreign key type mismatch
language_id: int = Field(foreign_key="public.language.language_id")

# ✅ Right - explicit SMALLINT type
language_id: int = Field(
    sa_column=Column(ForeignKey("public.language.language_id"), nullable=False),
    sa_type=SMALLINT
)
```

## 🔍 Why This Works

1. **UUIDs for business entities** = Globally unique, meaningful across systems
2. **SMALLINT for reference data** = Compact, fast lookups, human-readable IDs
3. **Type consistency** = Foreign keys match referenced primary key types
4. **Performance optimized** = Small reference tables use integer indexes
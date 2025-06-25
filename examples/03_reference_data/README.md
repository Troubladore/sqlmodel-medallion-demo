# Reference Data Patterns

## 🎯 Core Principles

1. **Soft deletes** via `inactivated_date` (NULL = active)
2. **Structured data loading** with ReferenceDataBase
3. **Merge-like behavior** for idempotent updates
4. **Dependency-aware ordering** (status types first)

## ✅ Complete Reference Table Pattern

```python
# src/pagila/tables/pagila/category.py
from datetime import datetime, UTC
from typing import Optional, List, TYPE_CHECKING

from sqlalchemy import SMALLINT, TIMESTAMP, Column
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .film_category import FilmCategory

class Category(SQLModel, table=True):
    __tablename__ = "category"
    __table_args__ = {"schema": "public"}

    # SMALLINT primary key for reference data
    category_id: int = Field(primary_key=True, sa_type=SMALLINT)
    name: str = Field(nullable=False, max_length=25)
    description: Optional[str] = Field(default=None, max_length=500)
    
    # ✅ Soft delete support - NULL = active
    inactivated_date: Optional[datetime] = Field(
        default=None, 
        sa_column=Column(TIMESTAMP(timezone=True), nullable=True)
    )
    
    # ✅ System audit timestamp
    systime: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )
    
    # Relationships
    film_categories: List["FilmCategory"] = Relationship(
        back_populates="category",
        sa_relationship_kwargs={"cascade": "save-update, merge"}
    )
```

## 🏗️ Reference Data Loading Pattern

### Base Data Handler

```python
# src/pagila/data/pagila/category__data.py
from datetime import datetime, UTC
from sqlmodel import Session

from src._base.data import ReferenceDataBase, ReferenceDataManager
from ..models import Category

class CategoryData(ReferenceDataBase[Category]):
    """Reference data handler for film categories"""

    def __init__(self):
        records = [
            Category(
                category_id=1,
                name="Action",
                description="High-energy films with physical stunts and chases",
                inactivated_date=None,  # NULL = active
                systime=datetime.now(UTC),
            ),
            Category(
                category_id=2,
                name="Animation", 
                description="Computer or hand-drawn animated films",
                inactivated_date=None,
                systime=datetime.now(UTC),
            ),
            Category(
                category_id=3,
                name="Children",
                description="Family-friendly content suitable for children",
                inactivated_date=None,
                systime=datetime.now(UTC),
            ),
            Category(
                category_id=4,
                name="Classics",
                description="Timeless films from earlier decades",
                inactivated_date=None,
                systime=datetime.now(UTC),
            ),
            Category(
                category_id=5,
                name="Comedy",
                description="Humorous films designed to entertain and amuse",
                inactivated_date=None,
                systime=datetime.now(UTC),
            ),
            # ... more categories
        ]

        super().__init__(
            model_class=Category,
            id_field="category_id",
            comparable_fields=[
                "name",
                "description", 
                "inactivated_date",  # ✅ Include for merge operations
            ],
            records=records,
        )

def populate_reference_data(session: Session) -> None:
    """Generic populate function discovered by the build system"""
    handler = CategoryData()
    manager = ReferenceDataManager(session)
    manager.merge_reference_data(handler)
```

### Language Reference Data

```python
# src/pagila/data/pagila/language__data.py
from datetime import datetime, UTC
from sqlmodel import Session

from src._base.data import ReferenceDataBase, ReferenceDataManager
from ..models import Language

class LanguageData(ReferenceDataBase[Language]):
    """Reference data handler for languages"""

    def __init__(self):
        records = [
            Language(
                language_id=1,
                name="English",
                iso_code="en",
                inactivated_date=None,
                systime=datetime.now(UTC),
            ),
            Language(
                language_id=2,
                name="Italian",
                iso_code="it", 
                inactivated_date=None,
                systime=datetime.now(UTC),
            ),
            Language(
                language_id=3,
                name="Japanese",
                iso_code="ja",
                inactivated_date=None,
                systime=datetime.now(UTC),
            ),
            Language(
                language_id=4,
                name="Mandarin",
                iso_code="zh",
                inactivated_date=None,
                systime=datetime.now(UTC),
            ),
            Language(
                language_id=5,
                name="French",
                iso_code="fr",
                inactivated_date=None,
                systime=datetime.now(UTC),
            ),
            # ... more languages
        ]

        super().__init__(
            model_class=Language,
            id_field="language_id", 
            comparable_fields=[
                "name",
                "iso_code",
                "inactivated_date",
            ],
            records=records,
        )

def populate_reference_data(session: Session) -> None:
    """Generic populate function discovered by the build system"""
    handler = LanguageData()
    manager = ReferenceDataManager(session)
    manager.merge_reference_data(handler)
```

## 🔄 Centralized Models Import

```python
# src/pagila/data/models.py
"""
Centralized imports for all Pagila database models.
Eliminates circular import issues for reference data files.
"""

# Reference tables
from ..tables.pagila.category import Category
from ..tables.pagila.language import Language
from ..tables.pagila.country import Country

# Business entities  
from ..tables.pagila.actor import Actor
from ..tables.pagila.customer import Customer
from ..tables.pagila.film import Film
from ..tables.pagila.staff import Staff

# Junction tables
from ..tables.pagila.film_actor import FilmActor
from ..tables.pagila.film_category import FilmCategory

__all__ = [
    # Reference data
    "Category",
    "Language", 
    "Country",
    # Business entities
    "Actor",
    "Customer",
    "Film", 
    "Staff",
    # Junction tables
    "FilmActor",
    "FilmCategory",
]
```

## 📋 Loading Priority System

Reference data files are loaded based on filename priority:

1. **`*status_type__data.py`** → Priority 1 (Load first)
2. **`*type__data.py`** → Priority 2 (Load second)  
3. **`*__data.py`** → Priority 3 (Load last)

### Example File Structure:
```
src/pagila/data/pagila/
├── rating_type__data.py           # Priority 2
├── language__data.py              # Priority 3  
├── category__data.py              # Priority 3
└── country__data.py               # Priority 3
```

## ⚙️ Soft Delete Operations

### Marking Records as Inactive
```python
# Instead of DELETE, set inactivated_date
category = session.get(Category, 5)
category.inactivated_date = datetime.now(UTC)
session.commit()
```

### Querying Active Records Only
```python
# Get only active categories
active_categories = session.exec(
    select(Category).where(Category.inactivated_date.is_(None))
).all()
```

### Reactivating Records
```python
# Reactivate a category
category = session.get(Category, 5)
category.inactivated_date = None
session.commit()
```

## 🛠️ Build System Integration

The reference data build system automatically:

1. **Discovers** files ending with `__data.py`
2. **Orders** by priority (status types → types → everything else)
3. **Imports** through centralized models.py (no circular imports)
4. **Calls** `populate_reference_data(session)` function
5. **Uses** ReferenceDataManager for merge-like behavior
6. **Handles** errors gracefully with CONTINUE_ON_ERROR

### Build Command:
```bash
.venv/Scripts/python.exe scripts/build_reference_data_improved.py postgres pagila INFO postgres localhost 5432
```

## ❌ Common Pitfalls

### DON'T: Forget inactivated_date
```python
# ❌ Missing soft delete support
class Category(SQLModel, table=True):
    category_id: int = Field(primary_key=True)
    name: str = Field(nullable=False)
    # Missing: inactivated_date field
```

### DON'T: Hard delete reference data
```python
# ❌ Hard delete loses historical context
session.delete(category)

# ✅ Soft delete preserves history
category.inactivated_date = datetime.now(UTC)
```

### DON'T: Skip comparable_fields
```python
# ❌ Missing inactivated_date in comparison
comparable_fields=["name", "description"]  # Missing inactivated_date

# ✅ Include all fields that affect record equality
comparable_fields=["name", "description", "inactivated_date"]
```

## 🎯 Key Benefits

- ✅ **Historical preservation** via soft deletes
- ✅ **Idempotent loading** via merge operations
- ✅ **Dependency resolution** via priority ordering
- ✅ **Type safety** with generics and proper imports
- ✅ **Error resilience** with graceful failure handling
- ✅ **Audit trails** with systime tracking
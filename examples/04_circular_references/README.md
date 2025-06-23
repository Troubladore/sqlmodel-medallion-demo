# Circular Reference Resolution

## 🎯 The Problem

SQLAlchemy can't resolve relationships when models have circular `TYPE_CHECKING` imports:

```
Category ←→ FilmCategory ←→ Film
```

**Error**: `"expression 'FilmCategory' failed to locate a name ('FilmCategory')"`

## ✅ Proven Solution: Centralized Models File

### 1. Create Central Models Import

```python
# src/pagila/data/models.py
"""
Centralized imports for all Pagila database models.
Eliminates circular import issues for reference data files.
"""

# Core entities
from ..tables.pagila.actor import Actor
from ..tables.pagila.category import Category
from ..tables.pagila.customer import Customer
from ..tables.pagila.film import Film
from ..tables.pagila.language import Language

# Junction tables
from ..tables.pagila.film_actor import FilmActor
from ..tables.pagila.film_category import FilmCategory

# All other tables...

__all__ = [
    "Actor",
    "Category", 
    "Customer",
    "Film",
    "FilmActor",
    "FilmCategory",
    "Language",
    # ... all exports
]
```

### 2. Individual Table Files Use TYPE_CHECKING

```python
# src/pagila/tables/pagila/category.py
from typing import TYPE_CHECKING, List
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .film_category import FilmCategory

class Category(SQLModel, table=True):
    __tablename__ = "category"
    __table_args__ = {"schema": "public"}
    
    category_id: int = Field(primary_key=True)
    name: str = Field(nullable=False)
    
    # ✅ Relationship with TYPE_CHECKING import
    film_categories: List["FilmCategory"] = Relationship(back_populates="category")
```

```python
# src/pagila/tables/pagila/film_category.py
from typing import TYPE_CHECKING, Optional
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .category import Category
    from .film import Film

class FilmCategory(SQLModel, table=True):
    __tablename__ = "film_category" 
    __table_args__ = {"schema": "public"}
    
    film_id: int = Field(primary_key=True)
    category_id: int = Field(primary_key=True)
    
    # ✅ Bidirectional relationships
    category: Optional["Category"] = Relationship(back_populates="film_categories")
    film: Optional["Film"] = Relationship(back_populates="film_categories")
```

### 3. Reference Data Uses Central Models

```python
# src/pagila/data/pagila/category__data.py
from datetime import datetime, UTC
from sqlmodel import Session

# ✅ Import from centralized models file
from ..models import Category

def populate_reference_data(session: Session) -> None:
    """Populate category reference data."""
    
    categories = [
        {"category_id": 1, "name": "Action"},
        {"category_id": 2, "name": "Animation"},
        # ... more categories
    ]
    
    for cat_data in categories:
        existing = session.get(Category, cat_data["category_id"])
        if not existing:
            category = Category(**cat_data, last_update=datetime.now(UTC))
            session.add(category)
    
    session.commit()
```

## 🔄 Complete Working Example

### Film Table
```python
# src/pagila/tables/pagila/film.py
from typing import TYPE_CHECKING, List
from uuid import UUID
import uuid

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .film_actor import FilmActor
    from .film_category import FilmCategory
    from .language import Language

class Film(SQLModel, table=True):
    __tablename__ = "film"
    __table_args__ = {"schema": "public"}

    film_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    title: str = Field(nullable=False)
    language_id: int = Field(foreign_key="public.language.language_id")

    # ✅ All relationships use TYPE_CHECKING imports
    language: Optional["Language"] = Relationship(back_populates="films")
    film_actors: List["FilmActor"] = Relationship(back_populates="film")
    film_categories: List["FilmCategory"] = Relationship(back_populates="film")
```

### Actor Table
```python
# src/pagila/tables/pagila/actor.py
from typing import TYPE_CHECKING, List
from uuid import UUID
import uuid

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .film_actor import FilmActor

class Actor(SQLModel, table=True):
    __tablename__ = "actor"
    __table_args__ = {"schema": "public"}

    actor_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    first_name: str = Field(nullable=False)
    last_name: str = Field(nullable=False)

    # ✅ Relationship through junction table
    film_actors: List["FilmActor"] = Relationship(back_populates="actor")
```

### Junction Table
```python
# src/pagila/tables/pagila/film_actor.py
from typing import TYPE_CHECKING, Optional
from uuid import UUID
import uuid

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .actor import Actor
    from .film import Film

class FilmActor(SQLModel, table=True):
    __tablename__ = "film_actor"
    __table_args__ = {"schema": "public"}

    # Junction table with UUID primary key
    film_actor_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    actor_id: UUID = Field(foreign_key="public.actor.actor_id")
    film_id: UUID = Field(foreign_key="public.film.film_id")

    # ✅ Bidirectional relationships
    actor: Optional["Actor"] = Relationship(back_populates="film_actors")
    film: Optional["Film"] = Relationship(back_populates="film_actors")
```

## 🛠️ Import Resolution Flow

1. **Individual table files** use `TYPE_CHECKING` for relationships
2. **Central models.py** imports all tables at runtime 
3. **Reference data files** import from models.py
4. **SQLAlchemy mapper initialization** happens after all models loaded
5. **Relationships resolve correctly** because all classes are available

## ❌ What NOT To Do

### DON'T: Runtime imports in table files
```python
# ❌ This creates circular imports
from .film_category import FilmCategory  # At module level

class Category(SQLModel, table=True):
    film_categories: List[FilmCategory] = Relationship(...)
```

### DON'T: Import individual table files in reference data
```python
# ❌ This triggers the circular import chain
from src.pagila.tables.pagila.category import Category

def populate_reference_data(session: Session):
    # This will fail with relationship resolution errors
    category = Category(...)
```

## ✅ Migration Steps

1. **Create** `src/pagila/data/models.py` with all table imports
2. **Update** reference data files to import from `models.py`
3. **Verify** all table files use `TYPE_CHECKING` pattern
4. **Test** reference data loading works without mapper errors

## 🎯 Key Benefits

- ✅ **Eliminates circular imports** completely
- ✅ **Preserves type safety** with TYPE_CHECKING
- ✅ **Enables clean relationships** with proper back_populates
- ✅ **Supports complex domain models** with many interconnections
- ✅ **Makes reference data loading reliable** and predictable
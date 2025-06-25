# SQLModel Medallion Architecture - Design Pattern Examples

This directory contains **proven patterns** extracted from working implementations, adapted for the Pagila domain. These patterns solve common issues like circular imports, UUID management, temporal tables, and reference data.

## 📁 Directory Structure

```
examples/
├── 01_uuid_patterns/          # Primary key and UUID best practices
├── 02_temporal_patterns/       # Business vs system time modeling
├── 03_reference_data/          # Lookup tables with soft deletes
├── 04_circular_references/     # Relationship management without circular imports
├── 05_table_type_mixins/       # Base classes for consistent patterns
└── 06_medallion_layers/        # Bronze/Silver/Gold layer patterns
```

## 🎯 Key Pattern Solutions

### ✅ Circular Import Resolution
- **Problem**: `FilmCategory` ↔ `Film` ↔ `Category` circular TYPE_CHECKING imports
- **Solution**: Centralized `models.py` file + proper import structure

### ✅ UUID vs Integer Primary Keys
- **Business Entities** (Customer, Film, Rental) → **UUID primary keys**
- **Reference Tables** (Category, Language, Rating) → **SMALLINT primary keys**

### ✅ Temporal Field Distinction
- **`systime`** = System audit time (when record created/modified)
- **`effective_time`** = Business effective time (when data is effective for business)

### ✅ Reference Data Management
- **Soft deletes** via `inactivated_date: Optional[datetime]` (NULL = active)
- **ReferenceDataBase** + **ReferenceDataManager** for consistent loading
- **Merge-like behavior** for idempotent reference data updates

### ✅ SQLModel Field Patterns
- **Don't mix**: `Field(nullable=True, sa_column=Column(..., nullable=True))`
- **Correct**: `Field(sa_column=Column(..., nullable=True))` OR `Field(nullable=True)`

## 🚀 Usage

Each example folder contains:
- ✅ **Working implementations** (tested patterns)
- 📚 **Detailed explanations** of why each pattern works
- 🔗 **Cross-references** to actual Pagila schema usage
- ⚠️ **Common pitfalls** and how to avoid them

Start with `01_uuid_patterns/` for foundational concepts, then progress through each pattern as needed.
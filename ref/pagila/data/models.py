"""
Centralized imports for all Pagila database models.

This module provides a single import point for all table classes in the pagila database,
eliminating the need for long import paths in reference data files and preventing
circular import issues when SQLAlchemy tries to resolve relationships.

Usage:
    from .models import Category, Language, Film, etc.
"""

# Reference tables (lookup data)
from ..tables.pagila.category import Category
from ..tables.pagila.country import Country
from ..tables.pagila.language import Language

# Core business entities
from ..tables.pagila.actor import Actor
from ..tables.pagila.customer import Customer
from ..tables.pagila.film import Film
from ..tables.pagila.staff import Staff
from ..tables.pagila.store import Store

# Location hierarchy
from ..tables.pagila.address import Address
from ..tables.pagila.city import City

# Film-related entities
from ..tables.pagila.inventory import Inventory

# Transaction entities
from ..tables.pagila.payment import Payment
from ..tables.pagila.rental import Rental

# Junction/relationship tables
from ..tables.pagila.film_actor import FilmActor
from ..tables.pagila.film_category import FilmCategory

# Export all models for easy importing
__all__ = [
    # Reference tables
    "Category",
    "Country", 
    "Language",
    # Core business entities
    "Actor",
    "Customer",
    "Film",
    "Staff",
    "Store",
    # Location hierarchy
    "Address",
    "City",
    # Film-related
    "Inventory",
    # Transactions
    "Payment",
    "Rental",
    # Junction tables
    "FilmActor",
    "FilmCategory",
]
from datetime import datetime, UTC
from sqlmodel import Session

from ..models import Category


def populate_reference_data(session: Session) -> None:
    """Populate category reference data."""
    
    categories = [
        {"category_id": 1, "name": "Action"},
        {"category_id": 2, "name": "Animation"},
        {"category_id": 3, "name": "Children"},
        {"category_id": 4, "name": "Classics"},
        {"category_id": 5, "name": "Comedy"},
        {"category_id": 6, "name": "Documentary"},
        {"category_id": 7, "name": "Drama"},
        {"category_id": 8, "name": "Family"},
        {"category_id": 9, "name": "Foreign"},
        {"category_id": 10, "name": "Games"},
        {"category_id": 11, "name": "Horror"},
        {"category_id": 12, "name": "Music"},
        {"category_id": 13, "name": "New"},
        {"category_id": 14, "name": "Sci-Fi"},
        {"category_id": 15, "name": "Sports"},
        {"category_id": 16, "name": "Travel"},
    ]
    
    for cat_data in categories:
        # Check if category already exists
        existing = session.get(Category, cat_data["category_id"])
        if not existing:
            category = Category(
                category_id=cat_data["category_id"],
                name=cat_data["name"],
                last_update=datetime.now(UTC)
            )
            session.add(category)
    
    session.commit()
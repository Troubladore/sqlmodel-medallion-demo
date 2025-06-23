from datetime import datetime, UTC
from sqlmodel import Session

from ..models import Country


def populate_reference_data(session: Session) -> None:
    """Populate country reference data."""
    
    countries = [
        {"country_id": 1, "country": "Afghanistan"},
        {"country_id": 2, "country": "Algeria"},
        {"country_id": 3, "country": "American Samoa"},
        {"country_id": 4, "country": "Angola"},
        {"country_id": 5, "country": "Anguilla"},
        {"country_id": 6, "country": "Argentina"},
        {"country_id": 7, "country": "Armenia"},
        {"country_id": 8, "country": "Australia"},
        {"country_id": 9, "country": "Austria"},
        {"country_id": 10, "country": "Azerbaijan"},
        # Add more countries as needed...
    ]
    
    for country_data in countries:
        # Check if country already exists
        existing = session.get(Country, country_data["country_id"])
        if not existing:
            country = Country(
                country_id=country_data["country_id"],
                country=country_data["country"],
                last_update=datetime.now(UTC)
            )
            session.add(country)
    
    session.commit()
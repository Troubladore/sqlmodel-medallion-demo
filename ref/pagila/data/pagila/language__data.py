from datetime import datetime, UTC
from sqlmodel import Session

from ..models import Language


def populate_reference_data(session: Session) -> None:
    """Populate language reference data."""
    
    languages = [
        {"language_id": 1, "name": "English"},
        {"language_id": 2, "name": "Italian"},
        {"language_id": 3, "name": "Japanese"},
        {"language_id": 4, "name": "Mandarin"},
        {"language_id": 5, "name": "French"},
        {"language_id": 6, "name": "German"},
    ]
    
    for lang_data in languages:
        # Check if language already exists
        existing = session.get(Language, lang_data["language_id"])
        if not existing:
            language = Language(
                language_id=lang_data["language_id"],
                name=lang_data["name"],
                last_update=datetime.now(UTC)
            )
            session.add(language)
    
    session.commit()
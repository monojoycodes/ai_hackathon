from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional
from datetime import date

# This is your "AIKosh Gatekeeper"
class AIKoshMetadata(BaseModel):
    title: str = Field(..., min_length=5)
    description: str
    sector: str
    organization: str
    format: List[str]
    url: HttpUrl
    last_updated: date
    is_machine_readable: bool = True

# Example of how this cleans your data
raw_data = {
    "title": "Rainfall Data 2023",
    "description": "Monthly rainfall stats for Nagpur district.",
    "sector": "Agriculture",
    "organization": "IMD",
    "format": ["CSV"],
    "url": "https://data.gov.in/sample.csv",
    "last_updated": "2023-12-31" # Pydantic will auto-convert this string to a date object
}

dataset = AIKoshMetadata(**raw_data)
print(dataset.model_dump_json(indent=2))
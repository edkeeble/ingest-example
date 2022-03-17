# Data classes
from typing import Dict
from pydantic import BaseModel

class SpireItem(BaseModel):
    id: int
    name: str
    location: str


class PlanetItem(BaseModel):
    id: int
    name: str
    something: str


class StacItem(BaseModel):
    id: int
    properties: Dict


class Nothing(BaseModel):
    pass

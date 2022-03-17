# Data classes
from typing import Dict
from pydantic import BaseModel


class SpireItem(BaseModel):
    id: str
    name: str
    location: int


class PlanetItem(BaseModel):
    id: int
    name: str
    something: str


class StacItem(BaseModel):
    id: str
    properties: Dict


class Nothing(BaseModel):
    pass

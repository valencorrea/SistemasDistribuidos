from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Actor:
    id: int
    name: str
    movie_id: int

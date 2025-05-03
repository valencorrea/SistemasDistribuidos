from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Movie:
    id:int
    title: str
    production_countries: list[str]
    release_date: str
    type: str
    genres: List[str]
    budget: int
    overview: str
    revenue: int

    def angentinian_production(self) -> bool:
        if not self.production_countries:
            return False
        try:
            return self.is_argentine()
        except Exception as e:
            print(f"[MOVIE] Error obteniendo producciones en {self.title}: {e}")
            return False

    def released_in_or_after_2000(self) -> bool:
        if not self.release_date:
            return False
        try:
            return int(self.release_date) >= 2000
        except Exception as e:
            print(f"[MOVIE] Error parseando fecha '{self.release_date}' en {self.title}: {e}")
            return False

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "production_countries": self.production_countries,
            "release_date": self.release_date,
            "type": self.type,
            "genres": self.genres,
            "budget": self.budget,
            "overview": self.overview,
            "revenue": self.revenue
        }

    def to_dict_title(self):
        return {
            "id": self.id,
            "title": self.title,
        }
    
    def get(self, attr: str):
        return getattr(self, attr)

    def is_argentine(self) -> bool:
        return 'AR' in self.production_countries
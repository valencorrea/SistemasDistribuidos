from dataclasses import dataclass
from typing import Optional

@dataclass
class Movie:
    title: str
    production_countries: list[str]
    release_date: str

    def released_in_or_after_2000(self) -> bool:
        if not self.release_date:
            return False
        try:
            return int(self.release_date) > 2000
        except Exception as e:
            print(f"[MOVIE] Error parseando fecha '{self.release_date}' en {self.title}: {e}")
            return False
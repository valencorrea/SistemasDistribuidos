from dataclasses import dataclass
from typing import Optional

@dataclass
class Movie:
    title: str
    release_date: Optional[str]

    def released_in_or_after_2000(self) -> bool:
        if not self.release_date:
            return False
        try:
            year = int(self.release_date.split("-")[0])
            return year >= 2000
        except Exception as e:
            print(f"[MOVIE] Error parseando fecha '{self.release_date}' en {self.title}: {e}")
            return False
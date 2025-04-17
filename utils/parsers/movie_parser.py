import csv
from model.movie import Movie
import ast

def convert_data(data):
    # data debe ser una lista de strings (líneas), no un string completo
    lines = data.get("cola", [])  # extrae lista de líneas desde el dict

    reader = csv.DictReader(lines, fieldnames=[
        "adult", "belongs_to_collection", "budget", "genres", "homepage", "id", "imdb_id",
        "original_language", "original_title", "overview", "popularity", "poster_path",
        "production_companies", "production_countries", "release_date", "revenue", "runtime",
        "spoken_languages", "status", "tagline", "title", "video", "vote_average", "vote_count"
    ])

    # ir sumando los campos a medida que se usan
    return [
        Movie(title=row["title"],
              production_countries=parse_production_countries(row["production_countries"]),
              release_date=parse_release_date(row["release_date"]))
        for row in reader
    ]


def parse_production_countries(data):
    try:
        countries = ast.literal_eval(data)
        return [c["iso_3166_1"] for c in countries if "iso_3166_1" in c]
    except Exception as e:
        print(f"[PARSE] Error parsing production_countries: {data} -> {e}")
        return []

def parse_release_date(data):
    try:
        return data.split("-")[0] if data else None
    except Exception as e:
        print(f"[PARSE] Error parsing release_date: {data} -> {e}")
        return None

import csv
from model.movie import Movie

def convert_data(data):
    # data debe ser una lista de strings (líneas), no un string completo
    lines = data.get("cola", [])  # extrae lista de líneas desde el dict

    reader = csv.DictReader(lines, fieldnames=[
        "adult", "belongs_to_collection", "budget", "genres", "homepage", "id", "imdb_id",
        "original_language", "original_title", "overview", "popularity", "poster_path",
        "production_companies", "production_countries", "release_date", "revenue", "runtime",
        "spoken_languages", "status", "tagline", "title", "video", "vote_average", "vote_count"
    ])

    return [
        Movie(title=row["title"], release_date=row["release_date"])
        for row in reader
    ]

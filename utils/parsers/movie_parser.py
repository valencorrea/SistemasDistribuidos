import csv
from model.movie import Movie

def convert_data(data):
    # data debe ser una lista de strings (líneas), no un string completo
    lines = data.get("cola", [])  # extrae lista de líneas desde el dict

    reader = csv.DictReader(lines, fieldnames=[
        "id", "title", "release_date", "color"
    ])

    return [
        Movie(title=row["title"], release_date=row["release_date"])
        for row in reader
    ]

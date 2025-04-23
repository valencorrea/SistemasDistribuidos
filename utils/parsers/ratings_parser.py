import csv
from model.movie import Movie
import ast
import json

def convert_data_for_rating_joiner(data):
    # data debe ser una lista de strings (líneas), no un string completo
    lines = data.get("cola", [])  # extrae lista de líneas desde el dict
    
    # Ignorar la primera línea que contiene los encabezados
    if lines and lines[0].startswith("userId,"):
        lines = lines[1:]

    reader = csv.DictReader(lines, fieldnames=[
        "userId","movieId","rating","timestamp"
    ])

    # ir sumando los campos a medida que se usan
    result = []
    for row in reader:
        if row["movieId"] and row["rating"] and is_float(row["rating"]):
            result.append({
                "movieId":row["movieId"],
                "rating":row["rating"],
            })
    return result

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
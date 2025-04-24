import csv
from model.movie import Movie
import ast
import json

def convert_data_for_main_movie_filter(data):
    # data debe ser una lista de strings (líneas), no un string completo
    lines = data.get("cola", [])  # extrae lista de líneas desde el dict
    
    # Ignorar la primera línea que contiene los encabezados
    if lines and lines[0].startswith("adult,"):
        lines = lines[1:]

    reader = csv.DictReader(lines, fieldnames=[
        "adult", "belongs_to_collection", "budget", "genres", "homepage", "id", "imdb_id",
        "original_language", "original_title", "overview", "popularity", "poster_path",
        "production_companies", "production_countries", "release_date", "revenue", "runtime",
        "spoken_languages", "status", "tagline", "title", "video", "vote_average", "vote_count"
    ])

    # ir sumando los campos a medida que se usan
    result = []
    for row in reader:
        if (row["id"] and 
            row["genres"] and 
            row["production_countries"] and
            row["release_date"] and 
            row["title"] and 
            row["production_countries"] != "[]" and
            row["budget"] and
            row["overview"] and
            row["revenue"]):
            result.append(Movie(
                id=row["id"],
                title=row["title"],
                production_countries=parse_production_countries(row["production_countries"]),
                release_date=parse_release_date(row["release_date"]),
                type="movie",
                genres=parse_genres(row["genres"]),
                overview=row["overview"],
                revenue=row["revenue"],
                budget=row["budget"]
            ))
    return result


def convert_data(data):
    # data debe ser una lista de strings (líneas), no un string completo
    lines = data.get("movies", [])  # extrae lista de líneas desde el dict


    # ir sumando los campos a medida que se usan
    result = []
    for row in lines:
        if (row["id"] and 
            row["genres"] and 
            row["production_countries"] and
            row["release_date"] and 
            row["title"] and 
            row["production_countries"] != "[]" and
            row["budget"] and
            row["overview"] and
            row["revenue"]):
            result.append(Movie(
                id=row["id"],
                title=row["title"],
                production_countries=row["production_countries"],
                release_date=row["release_date"],
                type="movie",
                genres=row["genres"],
                overview=row["overview"],
                revenue=row["revenue"],
                budget=row["budget"]
            ))
    return result


def convert_data_for_second_filter(data):
    # data debe ser una lista de strings (líneas), no un string completo
    lines = data.get("movies", [])  # extrae lista de líneas desde el dict
    

    # ir sumando los campos a medida que se usan
    result = []
    for row in lines:
        result.append({
            "production_countries":row["production_countries"],
            "budget":row["budget"]
        })
    return result

def convert_data_for_fifth_filter(data):
    # data debe ser una lista de strings (líneas), no un string completo
    lines = data.get("movies", [])  # extrae lista de líneas desde el dict

    # ir sumando los campos a medida que se usan
    result = []
    for row in lines:
        revenue = parse_revenue(row["revenue"])
        budget = parse_budget(row["budget"])
        if revenue==0 or budget==0:
            continue
        result.append({
            "overview":row["overview"],
            "budget":budget,
            "revenue":revenue
        })
    return result

def parse_revenue(data):
    try:
        return int(data)
    except:
        return 0

def parse_budget(data):
    try:
        return int(data)
    except:
        return 0

def parse_production_countries(data):
    try:
        # Primero intentamos con ast.literal_eval
        try:
            countries = ast.literal_eval(data)
        except:
            # Si falla, intentamos con json.loads
            countries = json.loads(data)
        
        if isinstance(countries, list):
            return [c["iso_3166_1"] for c in countries if isinstance(c, dict) and "iso_3166_1" in c]
        return []
    except Exception as e:
        print(f"[PARSE] Error parsing production_countries: {data} -> {e}")
        return []

def parse_release_date(data):
    try:
        if not data or data == "release_date":
            return None
        # Verificamos que la fecha tenga el formato correcto (YYYY-MM-DD)
        parts = data.split("-")
        if len(parts) == 3 and parts[0].isdigit():
            return parts[0]
        return None
    except Exception as e:
        print(f"[MOVIE] Error parseando fecha '{data}' en title: {e}")
        return None

def parse_genres(data):
    try:
        # Primero intentamos con ast.literal_eval
        try:
            genres = ast.literal_eval(data)
        except:
            # Si falla, intentamos con json.loads
            genres = json.loads(data)
        
        if isinstance(genres, list):
            return [g["name"] for g in genres if isinstance(g, dict) and "name" in g]
        return []
    except Exception as e:
        print(f"[PARSE] Error parsing genres: {data} -> {e}")
        return []

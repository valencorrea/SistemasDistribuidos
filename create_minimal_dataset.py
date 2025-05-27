import json
from datetime import datetime
from pathlib import Path

import pandas as pd


def make_movie(id, title, countries, year, genres, budget, revenue, overview):
    return {
        "id": id,
        "title": title,
        "genres": json.dumps([{"name": g} for g in genres]),
        "release_date": f"{year}-01-01",
        "production_countries": json.dumps([{"name": c} for c in countries]),
        "budget": budget,
        "revenue": revenue,
        "overview": overview,
        "spoken_languages": json.dumps([{"name": "English"}])
    }


def make_rating(movie_id, ratings):
    return [{"userId": uid, "movieId": movie_id, "rating": r, "timestamp": int(datetime.now().timestamp())}
            for uid, r in enumerate(ratings, start=1)]


def make_credits(movie_id, actors):
    return {
        "id": movie_id,
        "cast": json.dumps([{"name": name} for name in actors])
    }


def generate_reduced_valid_dataset(output_dir: str):
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    movies, ratings, credits = [], [], []
    actor_pool = [
        "Ricardo Darín", "Alejandro Awada", "Inés Efron", "Leonardo Sbaraglia", "Valeria Bertuccelli",
        "Arturo Goetz", "Diego Peretti", "Pablo Echarri", "Rafael Spregelburd", "Rodrigo de la Serna"
    ]
    country_budget = {
        "United States of America": 1_000_000_000,
        "France": 500_000_000,
        "United Kingdom": 400_000_000,
        "India": 300_000_000,
        "Japan": 200_000_000
    }

    movie_id = 1

    # --- Query 1: AR+ES coproductions (2000s)
    for i in range(3):
        movies.append(make_movie(
            id=movie_id,
            title=f"ArgSpanishCollab{i}",
            countries=["Argentina", "Spain"],
            year=2000 + i,
            genres=["Drama", "Comedy"],
            budget=10000,
            revenue=50000,
            overview="A deep story of cultural identity."
        ))
        ratings += make_rating(movie_id, [3.5, 4.0, 2.5])
        credits.append(make_credits(movie_id, actor_pool[i:i + 3]))
        movie_id += 1

    # --- Query 2: Solo country productions (big budgets)
    for country, budget in country_budget.items():
        movies.append(make_movie(
            id=movie_id,
            title=f"{country}Movie",
            countries=[country],
            year=2010,
            genres=["Action"],
            budget=budget,
            revenue=budget * 2,
            overview="Blockbuster action film."
        ))
        ratings += make_rating(movie_id, [4, 5])
        credits.append(make_credits(movie_id, [f"Actor from {country}"]))
        movie_id += 1

    # --- Query 3–4–5: Argentine solo movies post-2000
    ratings_data = [
        (["POSITIVE"], 4.0),
        (["NEGATIVE"], 1.0)
    ]
    for (sentiments, rating_val) in ratings_data:
        movies.append(make_movie(
            id=movie_id,
            title=f"ArgRating{sentiments[0]}",
            countries=["Argentina"],
            year=2015,
            genres=["Drama"],
            budget=1000,
            revenue=5000,
            overview="This movie has a POSITIVE outlook." if sentiments[0] == "POSITIVE" else "This movie has a NEGATIVE outlook."
        ))
        ratings += make_rating(movie_id, [rating_val])
        credits.append(make_credits(movie_id, actor_pool))
        movie_id += 1

    # Save datasets
    pd.DataFrame(movies).to_csv(output / "movies_metadata.csv", index=False)
    pd.DataFrame(ratings).to_csv(output / "ratings.csv", index=False)
    pd.DataFrame(credits).to_csv(output / "credits.csv", index=False)

    print(f"Dataset generated in {output}")


if __name__ == '__main__':
    generate_reduced_valid_dataset("./files/mini")

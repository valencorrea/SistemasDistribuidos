import json
import logging
import signal
import time
from pathlib import Path

import pandas as pd
import torch
from transformers import pipeline

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

def dictionary_to_list(value):
    try:
        return [d.get('name') for d in eval(value) if isinstance(d, dict) and 'name' in d]
    except Exception:
        return []

class Processor:
    def __init__(self):
        self.result = {}
        self.result_consumed = {}
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.close()

    def close(self):
        logger.info("Closing processor")

    def start(self):
        base_path = Path('./files')
        movies_path = base_path / 'movies_metadata.csv'
        ratings_path = base_path / 'ratings.csv'
        credits_path = base_path / 'credits.csv'

        if all(p.exists() for p in [movies_path, ratings_path, credits_path]):
            logger.info("Processing files...")
            self.result = {}  # reset
            self.process_files(movies_path, ratings_path, credits_path)

            output_folder = Path('./results')
            output_folder.mkdir(parents=True, exist_ok=True)
            with open(output_folder / 'results.json', 'w') as f:
                json.dump(self.result, f, indent=2)

    def process_files(self, movies_path, ratings_path, credits_path):
        movies_df = pd.read_csv(movies_path, low_memory=False)
        ratings_df = pd.read_csv(ratings_path)
        credits_df = pd.read_csv(credits_path)

        movies_df_columns = ["id", "title", "genres", "release_date", "overview", "production_countries",
                             "spoken_languages", "budget", "revenue"]
        ratings_df_columns = ["movieId", "rating", "timestamp"]
        credits_df_columns = ["id", "cast"]
        movies_df_cleaned = movies_df.dropna(subset=movies_df_columns)[movies_df_columns].copy()
        ratings_df_cleaned = ratings_df.dropna(subset=ratings_df_columns)[ratings_df_columns].copy()
        credits_df_cleaned = credits_df.dropna(subset=credits_df_columns)[credits_df_columns].copy()

        movies_df_cleaned['release_date'] = pd.to_datetime(movies_df_cleaned['release_date'], format='%Y-%m-%d', errors='coerce')
        ratings_df_cleaned['timestamp'] = pd.to_datetime(ratings_df_cleaned['timestamp'], unit='s')
        movies_df_cleaned['budget'] = pd.to_numeric(movies_df_cleaned['budget'], errors='coerce')
        movies_df_cleaned['revenue'] = pd.to_numeric(movies_df_cleaned['revenue'], errors='coerce')

        movies_df_cleaned['genres'] = movies_df_cleaned['genres'].apply(dictionary_to_list).astype(str)
        movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].apply(dictionary_to_list).astype(str)
        movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].apply(dictionary_to_list).astype(str)
        credits_df_cleaned['cast'] = credits_df_cleaned['cast'].apply(dictionary_to_list)

        # Q1
        q1 = movies_df_cleaned[
            (movies_df_cleaned['production_countries'].str.contains('Argentina', case=False, na=False)) &
            (movies_df_cleaned['production_countries'].str.contains('Spain', case=False, na=False)) &
            (movies_df_cleaned['release_date'].dt.year >= 2000) &
            (movies_df_cleaned['release_date'].dt.year < 2010)
        ]
        self.result[1] = q1[["title", "genres"]].to_dict(orient="records")

        # Q2
        solo_country_df = movies_df_cleaned[movies_df_cleaned['production_countries'].apply(lambda x: len(eval(x)) == 1)].copy()
        solo_country_df['country'] = solo_country_df['production_countries'].apply(lambda x: eval(x)[0])
        investment_by_country = solo_country_df.groupby('country')['budget'].sum().sort_values(ascending=False)
        self.result[2] = investment_by_country.head(5).to_dict()

        # Q3
        movies_argentina_post_2000_df = movies_df_cleaned[
            (movies_df_cleaned['production_countries'].str.contains('Argentina', case=False, na=False)) &
            (movies_df_cleaned['release_date'].dt.year >= 2000)
        ]
        movies_argentina_post_2000_df = movies_argentina_post_2000_df.astype({'id': int})
        ranking_arg_post_2000_df = movies_argentina_post_2000_df[["id", "title"]].merge(
            ratings_df_cleaned, left_on="id", right_on="movieId")
        mean_ranking_arg_post_2000_df = ranking_arg_post_2000_df.groupby(["id", "title"])['rating'].mean().reset_index()
        maxi = mean_ranking_arg_post_2000_df.iloc[mean_ranking_arg_post_2000_df['rating'].idxmax()]
        mini = mean_ranking_arg_post_2000_df.iloc[mean_ranking_arg_post_2000_df['rating'].idxmin()]
        self.result[3] = {'best': maxi.to_dict(), 'worst': mini.to_dict()}

        # Q4
        cast_arg_post_2000_df = movies_argentina_post_2000_df[["id", "title"]].merge(credits_df_cleaned, on="id")
        cast_and_movie_arg_post_2000_df = cast_arg_post_2000_df.set_index("id")["cast"].apply(pd.Series).stack().reset_index("id", name="name")
        cast_per_movie_quantities = cast_and_movie_arg_post_2000_df.groupby(["name"]).count().reset_index().rename(columns={"id": "count"})
        self.result[4] = cast_per_movie_quantities.nlargest(10, 'count').to_dict()

        # Q5
        q5_input_df = movies_df_cleaned.copy()
        q5_input_df = q5_input_df.loc[q5_input_df['budget'] != 0]
        q5_input_df = q5_input_df.loc[q5_input_df['revenue'] != 0]

        sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english', max_length=512, truncation=True)
        q5_input_df['sentiment'] = q5_input_df['overview'].fillna('').apply(lambda x: sentiment_analyzer(x)[0]['label'])
        q5_input_df["rate_revenue_budget"] = q5_input_df["revenue"] / q5_input_df["budget"]
        average_rate_by_sentiment = q5_input_df.groupby("sentiment")["rate_revenue_budget"].mean()
        self.result[5] = average_rate_by_sentiment.to_dict()

if __name__ == '__main__':
    processor = Processor()
    processor.start()

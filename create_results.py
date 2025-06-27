import json
import logging
import os
import signal
import time
from pathlib import Path

import pandas as pd
import torch
from transformers import pipeline

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
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.DEBUG,
            datefmt='%H:%M:%S')

    def exit_gracefully(self, signum, frame):
        self.close()

    def close(self):
        self.logger.info("Closing processor")

    def start(self):
        self.logger.info("Starting test results generator")
        base_path = Path('./files')
        output_folder = Path('./results')
        output_folder.mkdir(parents=True, exist_ok=True)
        found_any = False

        for dirpath, dirnames, filenames in os.walk(base_path):
            dir_path = Path(dirpath)
            movies_path = dir_path / 'movies_metadata.csv'
            ratings_path = dir_path / 'ratings.csv'
            credits_path = dir_path / 'credits.csv'

            if all(p.exists() for p in [movies_path, ratings_path, credits_path]):
                self.logger.info(f"Processing files in {dir_path}...")
                try:
                    self.result = {}
                    self.process_files(movies_path, ratings_path, credits_path)
                    result_file = output_folder / f'{dir_path.relative_to(base_path)}/results.json'
                    result_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(result_file, 'w') as f:
                        json.dump(self.result, f, indent=2)
                except Exception as e:
                    self.logger.exception(f"Error processing files in {dir_path}")

                found_any = True

        if not found_any:
            self.logger.info("Didn't find all required files in any subdirectory of ./files.")
        self.logger.info("Test results generator finished")

    def process_files(self, movies_path, ratings_path, credits_path):
        movies_df = pd.read_csv(movies_path, low_memory=False)
        self.logger.info(movies_df.shape)
        ratings_df = pd.read_csv(ratings_path)
        self.logger.info(ratings_df.shape)
        credits_df = pd.read_csv(credits_path)
        self.logger.info(credits_df.shape)

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
        movies_df_cleaned['genres'] = movies_df_cleaned['genres'].apply(dictionary_to_list)
        movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].apply(dictionary_to_list)
        movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].apply(dictionary_to_list)
        credits_df_cleaned['cast'] = credits_df_cleaned['cast'].apply(dictionary_to_list)
        movies_df_cleaned['genres'] = movies_df_cleaned['genres'].astype(str)
        movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].astype(str)
        movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].astype(str)
        movies_argentina_españa_00s_df = movies_df_cleaned[
            (movies_df_cleaned['production_countries'].str.contains('Argentina', case=False, na=False)) &
            (movies_df_cleaned['production_countries'].str.contains('Spain', case=False, na=False)) &
            (movies_df_cleaned['release_date'].dt.year >= 2000) &
            (movies_df_cleaned['release_date'].dt.year < 2010)
            ]
        self.result[1] = movies_argentina_españa_00s_df[["title", "genres"]].to_dict(orient="records")
        solo_country_df = movies_df_cleaned[
            movies_df_cleaned['production_countries'].apply(lambda x: len(eval(x)) == 1)].copy()
        solo_country_df['country'] = solo_country_df['production_countries'].apply(lambda x: eval(x)[0])
        investment_by_country = solo_country_df.groupby('country')['budget'].sum().sort_values(ascending=False)
        top_5_countries = investment_by_country.head(5)
        self.result[2] = top_5_countries.to_dict()

        # QUERY 3 Películas de Producción Argentina estrenadas a partir del 2000, con mayor y con menor promedio de rating.
        movies_argentina_post_2000_df = movies_df_cleaned[
            (movies_df_cleaned['production_countries'].str.contains('Argentina', case=False, na=False)) &
            (movies_df_cleaned['release_date'].dt.year >= 2000)
            ]
        movies_argentina_post_2000_df = movies_argentina_post_2000_df.astype({'id': int})

        ranking_arg_post_2000_df = movies_argentina_post_2000_df[["id", "title"]].merge(ratings_df_cleaned,
                                                                                        left_on="id",
                                                                                        right_on="movieId")
        mean_ranking_arg_post_2000_df = ranking_arg_post_2000_df.groupby(["id", "title"])['rating'].mean().reset_index()

        maxi = mean_ranking_arg_post_2000_df.iloc[mean_ranking_arg_post_2000_df['rating'].idxmax()]
        mini = mean_ranking_arg_post_2000_df.iloc[mean_ranking_arg_post_2000_df['rating'].idxmin()]

        self.result[3] = (maxi.to_dict(), mini.to_dict())

        # QUERY 4
        cast_arg_post_2000_df = movies_argentina_post_2000_df[["id", "title"]].merge(credits_df_cleaned,
                                                                                     on="id")
        cast_and_movie_arg_post_2000_df = cast_arg_post_2000_df.set_index("id")["cast"].apply(
            pd.Series).stack().reset_index("id", name="name")
        cast_per_movie_quantities = cast_and_movie_arg_post_2000_df.groupby(["name"]).count().reset_index().rename(
            columns={"id": "count"})
        self.result[4] = cast_per_movie_quantities.nlargest(10, 'count').to_dict()
        self.logger.info(f"Cast per movie quantities: {cast_per_movie_quantities}")
        q5_input_df = movies_df_cleaned.copy()
        q5_input_df = q5_input_df.loc[q5_input_df['budget'] != 0]
        q5_input_df = q5_input_df.loc[q5_input_df['revenue'] != 0]
        self.logger.info("Iniciando análisis de sentimiento...")
        sentiment_analyzer = pipeline('sentiment-analysis',
                                      model='distilbert-base-uncased-finetuned-sst-2-english',
                                      max_length=512,
                                      truncation=True)

        start_time = time.time()
        self.logger.info("Procesando sentimientos de las descripciones...")
        q5_input_df['sentiment'] = q5_input_df['overview'].fillna('').apply(lambda x: sentiment_analyzer(x)[0]['label'])
        elapsed_time = time.time() - start_time
        self.logger.info(f"Análisis de sentimiento completado en {elapsed_time:.2f} segundos")
        q5_input_df["rate_revenue_budget"] = q5_input_df["revenue"] / q5_input_df["budget"]
        average_rate_by_sentiment = q5_input_df.groupby("sentiment")["rate_revenue_budget"].mean()
        self.result[5] = average_rate_by_sentiment.to_dict()

if __name__ == '__main__':
    processor = Processor()
    processor.start()

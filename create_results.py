import json
import logging
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

    def start(self):
        base_path = Path('./files')
        output_base = Path('./results')
        for folder in base_path.rglob('*'):
            logger.info(f"Checking folder: {folder}")
            if folder.__str__() != 'files/mini':
                continue
            if folder.is_dir():
                movies_path = folder / 'movies_metadata.csv'
                ratings_path = folder / 'ratings.csv'
                credits_path = folder / 'credits.csv'

                if all(p.exists() for p in [movies_path, ratings_path, credits_path]):
                    logger.info(f"Processing folder: {folder}")
                    self.result = {}  # reset for each folder
                    self.process_files(movies_path, ratings_path, credits_path)

                    relative_path = folder.relative_to(base_path)
                    output_folder = output_base / relative_path
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

        movies_df_cleaned['release_date'] = pd.to_datetime(movies_df_cleaned['release_date'], format='%Y-%m-%d',
                                                           errors='coerce')
        ratings_df_cleaned['timestamp'] = pd.to_datetime(ratings_df_cleaned['timestamp'], unit='s')
        movies_df_cleaned['budget'] = pd.to_numeric(movies_df_cleaned['budget'], errors='coerce')
        movies_df_cleaned['revenue'] = pd.to_numeric(movies_df_cleaned['revenue'], errors='coerce')

        movies_df_cleaned['genres'] = movies_df_cleaned['genres'].apply(dictionary_to_list).astype(str)
        movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].apply(
            dictionary_to_list).astype(str)
        movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].apply(dictionary_to_list).astype(
            str)
        credits_df_cleaned['cast'] = credits_df_cleaned['cast'].apply(dictionary_to_list)

        df = movies_df_cleaned
        df2 = ratings_df_cleaned
        df3 = credits_df_cleaned

        # Q1
        q1 = df[
            (df['production_countries'].str.contains('Argentina', case=False, na=False)) &
            (df['production_countries'].str.contains('Spain', case=False, na=False)) &
            (df['release_date'].dt.year >= 2000) &
            (df['release_date'].dt.year < 2010)
            ]
        self.result[1] = q1[["title", "genres"]].to_dict(orient="records")

        # Q2
        solo_country_df = df[df['production_countries'].apply(lambda x: len(eval(x)) == 1)].copy()
        solo_country_df['country'] = solo_country_df['production_countries'].apply(lambda x: eval(x)[0])
        investment_by_country = solo_country_df.groupby('country')['budget'].sum().sort_values(ascending=False)
        self.result[2] = investment_by_country.head(5).to_dict()

        # Q3
        post2000 = df[df['production_countries'].str.contains('Argentina', case=False, na=False) & (
                df['release_date'].dt.year >= 2000)]
        post2000 = post2000.astype({'id': int})
        merged = post2000[['id', 'title']].merge(df2, left_on="id", right_on="movieId")
        rating_avg = merged.groupby(["id", "title"])['rating'].mean().reset_index()
        self.result[3] = (rating_avg.loc[rating_avg['rating'].idxmax()].to_dict(),
                          rating_avg.loc[rating_avg['rating'].idxmin()].to_dict())

        # Q4
        cast_merged = post2000[["id", "title"]].merge(df3, on="id")
        cast_expanded = cast_merged.set_index("id")["cast"].apply(pd.Series).stack().reset_index("id", name="name")
        cast_counts = cast_expanded.groupby("name").count().reset_index().rename(columns={"id": "count"})
        self.result[4] = cast_counts.nlargest(10, 'count').to_dict()

        # Q5
        q5_df = df[df['budget'] != 0]
        q5_df = q5_df[q5_df['revenue'] != 0]
        device = 0 if torch.cuda.is_available() else -1
        sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english',
                                      max_length=512, truncation=True, device=device)
        q5_df['sentiment'] = q5_df['overview'].fillna('').apply(lambda x: sentiment_analyzer(x)[0]['label'])
        q5_df["rate_revenue_budget"] = q5_df["revenue"] / q5_df["budget"]
        avg_sentiment = q5_df.groupby("sentiment")["rate_revenue_budget"].mean()
        self.result[5] = avg_sentiment.to_dict()

if __name__ == '__main__':
    processor = Processor()
    processor.start()

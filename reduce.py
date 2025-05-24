from pathlib import Path
import pandas as pd

def create_reduced_dataset(input_dir: str, output_dir: str, sample_size: int = 30):
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Load datasets
    movies_df = pd.read_csv(input_path / "movies_metadata.csv", low_memory=False)
    ratings_df = pd.read_csv(input_path / "ratings.csv")
    credits_df = pd.read_csv(input_path / "credits.csv")

    # Clean and convert types
    movies_df = movies_df[movies_df['id'].apply(lambda x: str(x).isdigit())].copy()
    credits_df = credits_df[credits_df['id'].apply(lambda x: str(x).isdigit())].copy()
    movies_df['id'] = movies_df['id'].astype(int)
    credits_df['id'] = credits_df['id'].astype(int)
    ratings_df['movieId'] = ratings_df['movieId'].astype(int)

    # Dates and budgets
    movies_df['release_date'] = pd.to_datetime(movies_df['release_date'], errors='coerce')
    movies_df['budget'] = pd.to_numeric(movies_df['budget'], errors='coerce').fillna(0)
    movies_df['revenue'] = pd.to_numeric(movies_df['revenue'], errors='coerce').fillna(0)

    # Filter: valid movies with ratings & credits
    valid_ids = set(ratings_df['movieId']) & set(credits_df['id'])
    valid_movies_df = movies_df[movies_df['id'].isin(valid_ids)].copy()

    # Query 1: Argentina + Spain co-productions in 2000s
    def has_arg_spain(x):
        return 'argentina' in x.lower() and 'spain' in x.lower()
    valid_movies_df['production_countries'] = valid_movies_df['production_countries'].astype(str)
    arg_spain_2000s = valid_movies_df[
        valid_movies_df['production_countries'].apply(has_arg_spain) &
        (valid_movies_df['release_date'].dt.year >= 2000) &
        (valid_movies_df['release_date'].dt.year < 2010)
    ]

    if arg_spain_2000s.empty:
        raise ValueError("No Argentina + Spain co-productions from 2000s with ratings & credits found.")

    # Query 2: Countries with only one country and non-zero budget
    import ast
    def one_country_nonzero_budget(row):
        try:
            countries = ast.literal_eval(row['production_countries'])
            return isinstance(countries, list) and len(countries) == 1 and row['budget'] > 0
        except Exception:
            return False

    solo_country_valid = valid_movies_df[valid_movies_df.apply(one_country_nonzero_budget, axis=1)]

    if solo_country_valid.empty:
        raise ValueError("No movies with exactly one production country and non-zero budget found.")

    # Sample from the union of valid and required
    must_include_ids = set(arg_spain_2000s['id']) | set(solo_country_valid['id'])
    remaining_pool = valid_movies_df[~valid_movies_df['id'].isin(must_include_ids)]

    remaining_sample = remaining_pool.sample(
        max(0, sample_size - len(must_include_ids)),
        random_state=42
    )
    final_sample_df = pd.concat([arg_spain_2000s, solo_country_valid, remaining_sample]).drop_duplicates('id')
    final_ids = set(final_sample_df['id'])

    # Final filtered CSVs
    ratings_filtered = ratings_df[ratings_df['movieId'].isin(final_ids)]
    credits_filtered = credits_df[credits_df['id'].isin(final_ids)]

    final_sample_df.to_csv(output_path / "movies_metadata.csv", index=False)
    ratings_filtered.to_csv(output_path / "ratings.csv", index=False)
    credits_filtered.to_csv(output_path / "credits.csv", index=False)

    print(f"Dataset saved to {output_path} with {len(final_sample_df)} movies.")

if __name__ == '__main__':
    create_reduced_dataset("./files/long", "./files/mini", sample_size=0)

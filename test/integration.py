import ast
import logging
import signal
import time

import langid
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
from transformers import pipeline

from middleware.consumer.consumer import Consumer
from worker.worker import Worker

# Configuración del logging al inicio del archivo
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')


class TestClass(Worker):
    def __init__(self):
        logger.info("Iniciando Cliente...")
        self.result = {}
        self.result_consumed = {}
        self.result_consumer = Consumer("result_comparator", _message_handler=self._handle_message)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.close()

    def close(self):
        logger.info(f"Closing worker")
        self.result_consumer.close()

    def start(self):
        logger.info("Comenzando procesamiento de datos...")
        logger.info("Cargando archivos CSV...")
        movies_df = pd.read_csv('/root/files/movies_metadata.csv', low_memory=False)
        logger.info(f"Archivo movies_metadata.csv cargado. Dimensiones: {movies_df.shape}")

        ratings_df = pd.read_csv('/root/files/ratings.csv')
        logger.info(f"Archivo ratings.csv cargado. Dimensiones: {ratings_df.shape}")

        credits_df = pd.read_csv('/root/files/credits.csv')
        logger.info(f"Archivo credits.csv cargado. Dimensiones: {credits_df.shape}")

        # Agregar más logs en puntos importantes del procesamiento
        logger.info("Comenzando limpieza de datos...")

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

        # Corregimos la conversión a diccionario
        self.result[3] = (maxi.to_dict(), mini.to_dict())
        cast_arg_post_2000_df = movies_argentina_post_2000_df[["id", "title"]].merge(credits_df_cleaned,
                                                                                     on="id")
        cast_and_movie_arg_post_2000_df = cast_arg_post_2000_df.set_index("id")["cast"].apply(
            pd.Series).stack().reset_index("id", name="name")
        cast_per_movie_quantities = cast_and_movie_arg_post_2000_df.groupby(["name"]).count().reset_index().rename(
            columns={"id": "count"})
        self.result[4] = cast_per_movie_quantities.nlargest(10, 'count').to_dict()
        logger.info(f"Cast per movie quantities: {cast_per_movie_quantities}")
        q5_input_df = movies_df_cleaned.copy()
        q5_input_df = q5_input_df.loc[q5_input_df['budget'] != 0]
        q5_input_df = q5_input_df.loc[q5_input_df['revenue'] != 0]
        logger.info("Iniciando análisis de sentimiento...")
        sentiment_analyzer = pipeline('sentiment-analysis',
                                      model='distilbert-base-uncased-finetuned-sst-2-english',
                                      max_length=512,
                                      truncation=True)

        start_time = time.time()
        logger.info("Procesando sentimientos de las descripciones...")
        q5_input_df['sentiment'] = q5_input_df['overview'].fillna('').apply(lambda x: sentiment_analyzer(x)[0]['label'])
        elapsed_time = time.time() - start_time
        logger.info(f"Análisis de sentimiento completado en {elapsed_time:.2f} segundos")
        q5_input_df["rate_revenue_budget"] = q5_input_df["revenue"] / q5_input_df["budget"]
        average_rate_by_sentiment = q5_input_df.groupby("sentiment")["rate_revenue_budget"].mean()
        self.result[5] = average_rate_by_sentiment.to_dict()
        self.wait_for_result()

    def _handle_message(self, message):
        """Handler para procesar cada mensaje recibido"""
        logger.info(f"Mensaje recibido: {message.get('result_number')}")
        self.result_consumed[message.get("result_number")] = message.get("result")
        if len(self.result_consumed.keys()) == 5:
            logger.info("Todos los resultados recibidos. Comenzando comparación...")
            self.compare_results()
            # Cerramos la conexión ya que hemos recibido todos los resultados
            self.result_consumer.close()

    def wait_for_result(self):
        """Inicia el consumo de mensajes"""
        logger.info("Esperando resultados...")
        self.result_consumer.start_consuming()

    def compare_results(self):
        logger.info("Comparando resultados...")
        for key, value in self.result.items():
            print(f"""
            Result {key}: {value}
            Result consumed {key}: {self.result_consumed[key]}
            """)

        # Agrega el análisis flexible
        self.compare_results_flexible()

    def compare_results_flexible(self):
        logger.info("\n=== Análisis de Diferencias ===")

        # Comparación Resultado 1 (Películas Argentina-España 2000s)
        logger.info("\n1. Películas Argentina-España 2000s:")
        # Convertir los géneros calculados de string a lista para comparación justa
        calc_movies_clean = [
            {'title': m['title'], 'genres': eval(m['genres']) if isinstance(m['genres'], str) else m['genres']}
            for m in self.result[1]
        ]

        # Comparar película por película
        for calc_movie in calc_movies_clean:
            cons_movie = next((m for m in self.result_consumed[1] if m['title'] == calc_movie['title']), None)
            if cons_movie is None:
                logger.info(f"❌ Película falta en resultado consumido: {calc_movie['title']}")
                continue

            if set(calc_movie['genres']) != set(cons_movie['genres']):
                logger.info(f"❌ Diferencia en géneros para {calc_movie['title']}:")
                logger.info(f"   Calculado: {sorted(calc_movie['genres'])}")
                logger.info(f"   Consumido: {sorted(cons_movie['genres'])}")

        # Verificar películas extra en el resultado consumido
        extra_movies = [m['title'] for m in self.result_consumed[1]
                        if not any(cm['title'] == m['title'] for cm in calc_movies_clean)]
        if extra_movies:
            logger.info(f"❌ Películas extra en resultado consumido: {extra_movies}")

        # Comparación Resultado 2 (Presupuesto por país)
        logger.info("\n2. Presupuesto por país:")
        country_codes = {
            'US': 'United States of America',
            'FR': 'France',
            'GB': 'United Kingdom',
            'IN': 'India',
            'JP': 'Japan'
        }
        for item in self.result_consumed[2]:
            country_code = item['country']
            budget = item['total_budget']
            country_name = country_codes.get(country_code)
            calc_budget = self.result[2].get(country_name)
            if calc_budget:
                diff_percent = abs(calc_budget - budget) / calc_budget * 100
                logger.info(f"{country_name}: Diferencia del {diff_percent:.2f}%")

        # Comparación Resultado 3 (Mejor y peor película)
        logger.info("\n3. Mejor y peor película:")
        calc_best = self.result[3][0]
        cons_best = self.result_consumed[3]['best']
        calc_worst = self.result[3][1]
        cons_worst = self.result_consumed[3]['worst']

        logger.info("Mejor película:")
        if calc_best['title'] == cons_best['title'] and abs(calc_best['rating'] - cons_best['rating']) < 0.1:
            logger.info("✅ Coinciden")
        else:
            logger.info(f"Calculado: {calc_best['title']} ({calc_best['rating']})")
            logger.info(f"Consumido: {cons_best['title']} ({cons_best['rating']})")

        logger.info("Peor película:")
        if calc_worst['title'] == cons_worst['title'] and abs(calc_worst['rating'] - cons_worst['rating']) < 0.1:
            logger.info("✅ Coinciden")
        else:
            logger.info(f"Calculado: {calc_worst['title']} ({calc_worst['rating']})")
            logger.info(f"Consumido: {cons_worst['title']} ({cons_worst['rating']})")

        # Comparación Resultado 4 (Actores más frecuentes)
        logger.info("\n4. Actores más frecuentes:")
        calc_actors = {name: count for name, count in
                       zip(self.result[4]['name'].values(), self.result[4]['count'].values())}
        cons_actors = {actor[1]['name']: actor[1]['count'] for actor in self.result_consumed[4]}

        for actor in set(calc_actors.keys()) | set(cons_actors.keys()):
            calc_count = calc_actors.get(actor, 0)
            cons_count = cons_actors.get(actor, 0)
            if abs(calc_count - cons_count) > 0:
                logger.info(f"{actor}: Calculado({calc_count}) vs Consumido({cons_count})")

        # Comparación Resultado 5 (Análisis de sentimiento) - CONTEO ABSOLUTO
        logger.info("\n5. Análisis de sentimiento (valores absolutos):")
        calc_pos = self.result[5]['POSITIVE']
        calc_neg = self.result[5]['NEGATIVE']
        cons_pos = self.result_consumed[5]['POSITIVE']['revenue']
        cons_neg = self.result_consumed[5]['NEGATIVE']['revenue']

        logger.info("Valores calculados:")
        logger.info(f"POSITIVE: {calc_pos:.2f}")
        logger.info(f"NEGATIVE: {calc_neg:.2f}")

        logger.info("\nValores consumidos:")
        logger.info(f"POSITIVE: {cons_pos:.2f}")
        logger.info(f"NEGATIVE: {cons_neg:.2f}")

        logger.info("\nDiferencias absolutas:")
        logger.info(f"POSITIVE: {abs(calc_pos - cons_pos):.2f}")
        logger.info(f"NEGATIVE: {abs(calc_neg - cons_neg):.2f}")


def dictionary_to_list(dictionary_str):
    try:
        dictionary_list = ast.literal_eval(dictionary_str)
        return [data['name'] for data in dictionary_list]
    except (ValueError, SyntaxError):
        return []


if __name__ == '__main__':
    TestClass = TestClass()
    TestClass.start()

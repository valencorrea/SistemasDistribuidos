import logging

from transformers import pipeline

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data_for_fifth_filter
from worker.worker import Worker


logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

class SentimentAnalyzerFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("movie_2", _message_handler=self.handle_message)
        self.producer = Producer("aggregate_consulta_5")
        self.sentiment_analyzer = pipeline("sentiment-analysis",
                model="distilbert-base-uncased-finetuned-sst-2-english",
                max_length=512,
                truncation=True)

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):
        try:
            movies = convert_data_for_fifth_filter(message)
            filtered_movies = self.analyze_sentiments(movies)

            # Crear un mensaje con la información del batch
            batch_message = {
                "movies": filtered_movies,
                "batch_size": message.get("batch_size", 0),
                "total_batches": message.get("total_batches", 0),
                "type": "batch_result",
                "client_id": message.get("client_id")
            }
            self.producer.enqueue(batch_message)
        except Exception as e:
            logger.error(f"Error en análisis de sentimiento: {e}")

    def analyze_sentiment(self, text: str) -> str:
        if not text:
            return "NEUTRAL"
        try:
            # Truncar el texto si es muy largo
            words = text.split()
            if len(words) > 500:
                text = " ".join(words[:500])

            result = self.sentiment_analyzer(text)
            return result[0]["label"].upper()
        except Exception as e:
            logger.error(f"Error en análisis de sentimiento: {e}")
            return "NEUTRAL"

    def start(self):
        try:
            self.consumer.start_consuming()
        except Exception as e:
            logger.error(f"Error en análisis de sentimiento: {e}")

    def analyze_sentiments(self, movies):
        result = []
        for movie in movies:
            sentiment = self.analyze_sentiment(movie.get("overview", ""))
            result.append({"sentiment": sentiment, "budget": movie.get("budget"), "revenue": movie.get("revenue")})
        return result

if __name__ == '__main__':
    worker = SentimentAnalyzerFilter()
    worker.start()
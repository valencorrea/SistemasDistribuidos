import logging
import threading

from transformers import pipeline

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data_for_fifth_filter
from worker.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SentimentAnalyzerFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("movie_2", message_factory=self.handle_message)
        self.producer = Producer("aggregate_consulta_5")
        self.sentiment_analyzer = pipeline("sentiment-analysis",
                model="distilbert-base-uncased-finetuned-sst-2-english")

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):
        if not message:
            return None
        movies = convert_data_for_fifth_filter(message)
        filtered_movies = self.apply_filter(movies)

        # Crear un mensaje con la información del batch
        batch_message = {
            "movies": filtered_movies,
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

        return batch_message

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

        while not self.shutdown_event.is_set():
            message = self.consumer.dequeue()
            if not message:
                continue
            self.producer.enqueue(message)

    def apply_filter(self, movies):
        result = []
        for movie in movies:
            sentiment = self.analyze_sentiment(movie.get("overview", ""))
            result.append({"sentiment": sentiment, "budget": movie.get("budget"), "revenue": movie.get("revenue")})
        return result

if __name__ == '__main__':
    worker = SentimentAnalyzerFilter()
    worker.start()
from transformers import pipeline

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data_for_fifth_filter
from worker.filter.filter import Filter


class SentimentAnalyzerFilter(Filter):
    def __init__(self):
        super().__init__("sentiment_analyzer_filter", "movies")
        self.consumer = Consumer("movie_2", _message_handler=self.handle_message)
        self.producer = Producer("aggregate_consulta_5")
        self.sentiment_analyzer = pipeline("sentiment-analysis",
                                           model="distilbert-base-uncased-finetuned-sst-2-english",
                                           max_length=512,
                                           truncation=True)

    def create_consumer(self):
        return Consumer("movie_2", _message_handler=self.handle_message)

    def create_producers(self):
        return [Producer("aggregate_consulta_5")]

    def filter(self, message):
        try:
            movies = convert_data_for_fifth_filter(message)
            return self.analyze_sentiments(movies)
        except Exception as e:
            self.logger.error(f"Error en análisis de sentimiento: {e}")

    def analyze_sentiments(self, movies):
        result = []
        for movie in movies:
            if movie.get("budget") is None or movie.get("budget") == 0 or movie.get("revenue") is None or movie.get("revenue") == 0:
                self.logger.info(f"Skipped")
                continue
            sentiment = self.analyze_sentiment(movie.get("overview", ""))
            result.append({"sentiment": sentiment, "budget": movie.get("budget"), "revenue": movie.get("revenue")})
        return result

    def analyze_sentiment(self, text: str) -> str:
        if not text:
            return "NEUTRAL"
        try:
            words = text.split()
            if len(words) > 500:
                text = " ".join(words[:500])

            result = self.sentiment_analyzer(text)
            return result[0]["label"].upper()
        except Exception as e:
            self.logger.error(f"Error en análisis de sentimiento: {e}")
            return "NEUTRAL"


if __name__ == '__main__':
    worker = SentimentAnalyzerFilter()
    worker.start()

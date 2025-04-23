import json
from collections import defaultdict
import logging
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Aggregator:
    def __init__(self):
        self.consumer = Consumer("aggregate_consulta_5")  # Lee de la cola de resultados filtrados
        self.producer = Producer("result")  # Envía el resultado final
        self.total_batches = None
        self.received_batches = 0
        self.sentiment_revenue = defaultdict(float)  # Diccionario para sumar ganancias por senitmiento
        self.sentiment_budget = defaultdict(float)  # Diccionario para sumar presupuestos por sentimiento

    def process_sentiment_revenue_budget(self, movies):
        """Suma el presupuesto de las películas por país"""
        for movie in movies:
            if not movie:  # Si movie es None
                continue

            try:
                sentiment = movie.get("sentiment", "")
                revenue = float(movie.get("revenue", 0))
                budget = float(movie.get("budget", 0))

                if sentiment:  # Solo procesar si hay un país
                    self.sentiment_budget[sentiment] += budget
                    self.sentiment_revenue[sentiment] += revenue
            except (ValueError, TypeError) as e:
                logger.warning(f"Error procesando película: {e}")
                continue

    def _get_sentiment_mean(self):
        """obtiene el promedio de ganancias y presupuestos por sentimiento"""
        sentiment_mean = {}
        for sentiment in self.sentiment_budget:
            sentiment_mean[sentiment] = {
                "revenue": self.sentiment_revenue[sentiment] / self.sentiment_budget[sentiment]
            }
        return sentiment_mean

    def start(self):
        """Inicia el procesamiento de mensajes"""
        logger.info("Iniciando agregador")

        try:
            while True:
                message = self.consumer.dequeue()

                if not message:
                    continue

                if message.get("type") == "shutdown":
                    break

                if message.get("type") == "batch_result":
                    # Procesar las películas del batch
                    self.process_sentiment_revenue_budget(message.get("movies", []))
                    self.received_batches += message.get("batch_size", 0)

                    if message.get("total_batches"):
                        self.total_batches = message.get("total_batches")

                    logger.info(f"Batches recibidos: {self.received_batches}/{self.total_batches}")

                    # Si hemos recibido todos los batches, enviar el resultado final
                    if self.total_batches and self.total_batches > 0 and self.received_batches >= self.total_batches:
                        rate_revenue_budget = self._get_sentiment_mean()
                        result_message = {
                            "type": "result",
                            "rate_revenue_budget": rate_revenue_budget
                        }
                        if self.producer.enqueue(result_message):
                            logger.info("Resultado final enviado con top 5 países")
                        break

        except KeyboardInterrupt:
            logger.info("Deteniendo agregador...")
        finally:
            self.close()

    def close(self):
        """Cierra las conexiones"""
        self.consumer.close()
        self.producer.close()


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()
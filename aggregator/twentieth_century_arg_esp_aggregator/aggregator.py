import json
import logging
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Aggregator:
    def __init__(self):
        self.consumer = Consumer("aggregate_consulta_1")  # Lee de la cola de resultados filtrados
        self.producer = Producer("result")  # Envía el resultado final
        self.filtered_movies = []  # Almacena las películas filtradas
        self.total_batches = None
        self.received_batches = 0

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
                    # Acumular las películas del batch
                    self.filtered_movies.extend(message.get("movies", []))
                    self.received_batches += message.get("batch_size", 0)
                    
                    if message.get("total_batches"):
                        self.total_batches = message.get("total_batches")

                    logger.info(f"Batch procesado. Películas acumuladas: {len(self.filtered_movies)}")
                    logger.info(f"Batches recibidos: {self.received_batches}/{self.total_batches}")

                    # Si hemos recibido todos los batches, enviar el resultado final
                    if self.total_batches and self.total_batches > 0 and self.received_batches >= self.total_batches:
                        result_message = {
                            "type": "result",
                            "movies": self.filtered_movies,
                            "total_movies": len(self.filtered_movies)
                        }
                        if self.producer.enqueue(result_message):
                            logger.info(f"Resultado final enviado con {len(self.filtered_movies)} películas")
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
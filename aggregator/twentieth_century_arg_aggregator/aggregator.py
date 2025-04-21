import json
import logging
from collections import defaultdict
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Aggregator:
    def __init__(self):
        self.movies = None
        self.consumer = Consumer("partial_aggregator_4")  # Lee de la cola de resultados filtrados
        self.producer = Producer("result")  # Envía el resultado final
        self.country_budget = defaultdict(float)  # Diccionario para sumar presupuestos por país
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
                    # Procesar las películas del batch
                    self.movies = message.get("movies", [])
                    self.received_batches += message.get("batch_size", 0)
                    
                    if message.get("total_batches"):
                        self.total_batches = message.get("total_batches")

                    logger.info(f"Batch procesado. Países acumulados: {len(self.country_budget)}")
                    logger.info(f"Batches recibidos: {self.received_batches}/{self.total_batches}")


                    # Si hemos recibido todos los batches, enviar el resultado final
                    if self.total_batches and self.total_batches > 0 and self.received_batches >= self.total_batches:

                        if self.producer.enqueue("envieeeeeeeeeeee"):
                            logger.info("Resultado final enviado con top 5 países")
                            logger.info(self.movies)
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
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

               
                    
                if self.producer.enqueue(message):
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
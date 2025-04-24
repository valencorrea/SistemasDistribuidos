import logging
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("aggregate_consulta_2")  # Lee de la cola de resultados filtrados
        self.producer = Producer("result")  # Envía el resultado final
        self.country_budget = defaultdict(float)  # Diccionario para sumar presupuestos por país
        self.total_batches = None
        self.received_batches = 0

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def process_country_budget(self, movies):
        """Suma el presupuesto de las películas por país"""
        for movie in movies:
            if not movie:  # Si movie es None
                continue
            try:
                country = movie.get("country", "")
                budget = float(movie.get("budget", 0))
                
                if country:  # Solo procesar si hay un país
                    self.country_budget[country] += budget
            except (ValueError, TypeError) as e:
                logger.warning(f"Error procesando película: {e}")
                continue

    def _get_top_5_countries(self):
        """Obtiene el top 5 de países con mayor presupuesto"""
        # Ordenar países por presupuesto (de mayor a menor)
        sorted_countries = sorted(
            self.country_budget.items(),
            key=lambda x: x[1],
            reverse=True
        )
        # Tomar solo los primeros 5
        top_5 = sorted_countries[:5]
        # Crear la estructura final
        return [
            {
                "country": country,
                "total_budget": budget
            }
            for country, budget in top_5
        ]

    def start(self):
        """Inicia el procesamiento de mensajes"""
        logger.info("Iniciando agregador")
        
        try:
            while not self.shutdown_event.is_set():
                message = self.consumer.dequeue()
                if not message:
                    continue
                if message.get("type") == "batch_result":
                    # Procesar las películas del batch
                    self.process_country_budget(message.get("movies", []))
                    self.received_batches += message.get("batch_size", 0)
                    
                    if message.get("total_batches"):
                        self.total_batches = message.get("total_batches")

                    logger.info(f"Batch procesado. Países acumulados: {len(self.country_budget)}")
                    logger.info(f"Batches recibidos: {self.received_batches}/{self.total_batches}")

                    # Sí hemos recibido todos los batches, enviar el resultado final
                    if self.total_batches and 0 < self.total_batches <= self.received_batches:
                        top_5_countries = self._get_top_5_countries()
                        result_message = {
                            "type": "result",
                            "top_5_countries": top_5_countries
                        }
                        if self.producer.enqueue(result_message):
                            logger.info("Resultado final enviado con top 5 países")
                        break
        except Exception as e:
            logger.error(f"Error durante el procesamiento: {e}")
        finally:
            self.close()

if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start() 
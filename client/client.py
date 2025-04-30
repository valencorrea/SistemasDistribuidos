import signal
import threading
import logging
from typing import Generator

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker


logger = logging.getLogger(__name__)


class Client(Worker):
    def __init__(self):
        super().__init__()
        self.producer = Producer("movie_main_filter")
        self.actor_producer = Producer("credits")
        self.rating_producer = Producer("ratings")
        self.shutdown_producer = Producer("shutdown", "fanout")
        self.result_consumer = Consumer("result", _message_handler=self.wait_for_result)
        self.test_producer = Producer("result_test")
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.results_received = 0
        self.shutdown_event = threading.Event()

    def exit_gracefully(self, signum, frame):
        self.close()

    def wait_for_result(self, query_result):
        self.results_received += 1
        logger.info(f"[INFO] Resultado {self.results_received}/5 recibido: {query_result}")
        self.test_producer.enqueue(query_result)
        if self.results_received == 5:
            self.close()

    def close(self):
        logger.info(f"Closing all workers")
        self.shutdown_producer.enqueue("shutdown")
        self.producer.close()
        self.actor_producer.close()
        self.result_consumer.close()

    def send(self, message: dict) -> bool:
        try:
            return self.producer.enqueue(message)
        except Exception as e:
            logger.error(f"[ERROR] Error al enviar mensaje: {e}")
            return False

    def send_actor(self, message: dict) -> bool:
        try:
            return self.actor_producer.enqueue(message)
        except Exception as e:
            logger.error(f"[ERROR] Error al enviar mensaje: {e}")
            return False

    def send_rating(self, message: dict) -> bool:
        try:
            return self.rating_producer.enqueue(message)
        except Exception as e:
            logger.error(f"[ERROR] Error al enviar mensaje: {e}")
            return False

    @staticmethod
    def process_file(file_path: str, batch_size: int = 1000) -> Generator[tuple[list[str], bool], None, None]:
        try:
            with open(file_path, "r") as file:
                # Leer el encabezado
                header = next(file)
                current_batch = []
                # Leer la primera línea después del header
                line = next(file, None)

                while line is not None:
                    current_batch.append(line)

                    # Leer la siguiente línea para ver si es la última
                    next_line = next(file, None)
                    is_last = next_line is None

                    if len(current_batch) >= batch_size or is_last:
                        yield [header] + current_batch, is_last
                        current_batch = []

                    line = next_line
        finally:
            file.close()

    def start(self):
        logger.info("Comenzando con el envio de archivos")
        try:
            successful_batches = 0
            total_batches = 0
            logger.info("Envio de peliculas")
            for batch, is_last in client.process_file("root/files/movies_metadata.csv"):
                message = {
                    "type": "movie",
                    "cola": batch,
                    "batch_size": len(batch),
                    "total_batches": total_batches + len(batch) if is_last else 0
                }
                result = client.send(message)

                if result:
                    successful_batches += 1
                    logger.info(f"[MAIN] Batch {total_batches + 1} enviado correctamente")
                else:
                    logger.error(f"[ERROR] Falló el envío del batch {total_batches + 1}")
                total_batches += len(batch)

            successful_batches = 0
            total_batches = 0
            logger.info("Envio de creditos")
            for batch, is_last in client.process_file("root/files/credits.csv"):
                message = {
                    "type": "actor",
                    "cola": batch,
                    "batch_size": len(batch),
                    "total_batches": total_batches + len(batch) if is_last else 0
                }
                logger.info("enviando a send actor")
                result = client.send_actor(message)

                if result:
                    successful_batches += 1
                    logger.info(f"[MAIN] Batch {total_batches + 1} enviado correctamente")
                else:
                    logger.error(f"[ERROR] Falló el envío del batch {total_batches + 1}")
                total_batches += len(batch)
            successful_batches = 0
            total_batches = 0

            logger.info("Envio de ratings")
            for batch, is_last in client.process_file("root/files/ratings.csv", 100000):
                message = {
                    "type": "rating",
                    "cola": batch,
                    "batch_size": len(batch),
                    "total_batches": total_batches + len(batch) if is_last else 0
                }
                result = client.send_rating(message)

                if result:
                    successful_batches += 1
                    logger.info(f"[MAIN] Batch {total_batches + 1} enviado correctamente")
                else:
                    logger.error(f"[ERROR] Falló el envío del batch {total_batches + 1}")
                total_batches += len(batch)
            self.result_consumer.start_consuming()
            logger.info("Finalizo el envio de archivos")
        except Exception as e:
            logger.error(f"[ERROR] Error durante el procesamiento: {e}")
        finally:
            logger.info(f"\nEsperando resultados")
            self.shutdown_event.wait()
            logger.info(f"\nTerminado")
            # logger.info(f"Total de lotes procesados: {total_batches}")
            # logger.info(f"Lotes exitosos: {successful_batches}")
            # logger.info(f"Tasa de éxito: {(successful_batches/total_batches)*100:.2f}%")
            # client.close()


if __name__ == '__main__':
    client = Client()
    client.start()

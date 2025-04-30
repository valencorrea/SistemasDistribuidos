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
        self.test_producer = Producer("result_comparator")

        self.result_consumer = Consumer("result", _message_handler=self.wait_for_result)
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
        #self.shutdown_producer.enqueue("shutdown")
        self.producer.close()
        self.actor_producer.close()
        self.result_consumer.close()

    @staticmethod
    def send(message: dict, producer) -> bool:
        try:
            return producer.enqueue(message)
        except Exception as e:
            logger.error(f"[ERROR] Error al enviar mensaje: {e}")
            return False

    @staticmethod
    def process_file(file_path: str, batch_size: int) -> Generator[tuple[list[str], bool], None, None]:
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

    @staticmethod
    def send_file(file_path: str, batch_type: str, producer, batch_size: int = 1000):
        successful_batches = 0
        total_batches = 0
        logger.info("Sending " + batch_type + " batches")
        for batch, is_last in client.process_file(file_path, batch_size):
            message = {
                "type": batch_type,
                "cola": batch,
                "batch_size": len(batch),
                "total_batches": total_batches + len(batch) if is_last else 0
            }
            result = client.send(message, producer)

            if result:
                successful_batches += 1
                logger.info(f"[MAIN] Batch {total_batches + 1} enviado correctamente")
            else:
                logger.error(f"[ERROR] Falló el envío del batch {total_batches + 1}")
            total_batches += len(batch)

    def start(self):
        logger.info("Comenzando con el envio de archivos")
        try:
            self.send_file("root/files/movies_metadata.csv", "movie", self.producer)
            self.send_file("root/files/credits.csv", "actor", self.actor_producer)
            self.send_file("root/files/movies_metadata.csv", "movie", self.rating_producer, 100000)

            self.result_consumer.start_consuming()
            logger.info("Finalizo el envio de archivos")

        except Exception as e:
            logger.error(f"[ERROR] Error durante el procesamiento: {e}")
        finally:
            logger.info(f"\nEsperando resultados")
            self.shutdown_event.wait()
            logger.info(f"\nTerminado")


if __name__ == '__main__':
    client = Client()
    client.start()

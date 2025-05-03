import signal
import threading
import logging
import time
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from middleware.file_consuming.file_consuming import CSVReceiver
from worker.worker import Worker
import uuid

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

class ClientDecodifier(Worker):
    def __init__(self):
        super().__init__()
        self.csv_receiver = CSVReceiver()
        
        # Mantener los producers existentes
        self.producer = Producer("movie_main_filter")
        self.actor_producer = Producer("credits")
        self.rating_producer = Producer("ratings")
        self.test_producer = Producer("result_comparator")
        
        self.result_consumer = Consumer("result", _message_handler=self.wait_for_result)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.results_received = 0
        self.shutdown_event = threading.Event()

    def get_producer_for_type(self, file_type: str) -> Producer:
        if file_type == "movie":
            return self.producer
        elif file_type == "actor":
            return self.actor_producer
        elif file_type == "rating":
            return self.rating_producer
        return None

    def process_connection(self, client_socket, client_id: str):
        """Procesa los batches de una conexión"""
        try:
            total_batches = 0
            logger.info("Iniciando procesamiento de nueva conexión")
            
            for batch, is_last, metadata in self.csv_receiver.process_connection(client_socket):
                producer = self.get_producer_for_type(metadata.type)
                if not producer:
                    logger.error(f"Tipo de archivo no válido: {metadata.type}")
                    continue

                logger.debug(f"Preparando mensaje para enviar batch de tipo {metadata.type} con {len(batch)} líneas")
                
                message = {
                    "type": metadata.type,
                    "cola": batch,
                    "batch_size": len(batch),
                    "total_batches": total_batches + len(batch) if is_last else 0,
                    "client_id": client_id
                }
                
                if not self.send(message, producer):
                    logger.error(f"[ERROR] Falló el envío del batch {total_batches + 1} a RabbitMQ")
                
                total_batches += len(batch)

            logger.info(f"Procesamiento completado. Total de batches enviados: {total_batches}")
            
        except Exception as e:
            logger.error(f"Error en process_connection: {e}", exc_info=True)


    def start(self):

        logger.info("Iniciando servidor decodificador...")
        max_retries = 5
        retry_delay = 2
        consumer_thread = threading.Thread(target=self.result_consumer.start_consuming)
        consumer_thread.daemon = True
        consumer_thread.start()
        # Intentar iniciar el servidor
        for attempt in range(max_retries):
            try:
                logger.info(f"Intento {attempt + 1} de iniciar el servidor")
                if self.csv_receiver.start_server():
                    logger.info("Servidor iniciado exitosamente")
                    break
                else:
                    raise Exception("No se pudo iniciar el servidor")
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error iniciando servidor: {e}. Reintentando en {retry_delay} segundos...")
                    time.sleep(retry_delay)
                else:
                    logger.error("No se pudo iniciar el servidor después de varios intentos")
                    return

        # Procesar conexiones
        try:
            while True:
                logger.info("Esperando conexiones en el puerto 50000...")
                client_socket, addr = self.csv_receiver.accept_connection()
                if client_socket:
                    client_id = str(uuid.uuid4())
                    logger.info(f"Nueva conexión aceptada desde {addr}")
                    thread = threading.Thread(
                        target=self.process_connection,
                        args=(client_socket,client_id)
                    )
                    thread.daemon = True
                    thread.start()
                else:
                    logger.error("Error aceptando conexión")

        except KeyboardInterrupt:
            logger.info("Cerrando servidor...")
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
        finally:
            self.csv_receiver.close()
            self.shutdown_event.wait()

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


if __name__ == '__main__':
    decodifier = ClientDecodifier()
    decodifier.start()

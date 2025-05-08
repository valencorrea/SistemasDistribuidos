import signal
import threading
import logging
import time
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from middleware.file_consuming.file_consuming import CSVReceiver
from worker.worker import Worker

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%H:%M:%S')

class ClientDecodifier(Worker):
    def __init__(self):
        super().__init__()
        self.csv_receiver = CSVReceiver()
        self.client_sockets = {}
        self.client_sockets_lock = threading.Lock()

        self.test_producer = Producer("result_comparator")

        self.result_consumer = Consumer("result", _message_handler=self.wait_for_result)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.results_received = 0
        self.shutdown_event = threading.Event()

    def process_connection(self, client_socket):
        movie_producer = Producer("movie_main_filter")
        actor_producer = Producer("credits")
        rating_producer = Producer("ratings")
        try:
            logger.info("Iniciando procesamiento de nueva conexión")
            generated = self.csv_receiver.process_connection(client_socket)
            last_metadata = None
            total_batches = 0
            for client_id, batch, is_last, metadata in generated:
                with self.client_sockets_lock:
                    if client_id not in self.client_sockets:
                        self.client_sockets[client_id] = client_socket
                        logger.info("Socket: " + str(client_socket) + f" successfully added to client_sockets con client_id={client_id}")

                if metadata.type == "movie":
                    producer = movie_producer
                elif metadata.type == "credit":
                    producer = actor_producer
                elif metadata.type == "rating":
                    producer = rating_producer
                else:
                    logger.error(f"Metadata no reconocido")
                    return
                if last_metadata != metadata.type:
                    total_batches = 0
                last_metadata = metadata.type
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

                logger.debug(f"Enviando {metadata.type} de {client_id}")

                if is_last:
                    logger.info(f"Enviando ultimo batch de {metadata.type} de {client_id}")

                if not self.send(message, producer):
                    logger.error(f"[ERROR] Falló el envío del batch {total_batches + 1} a RabbitMQ")
                
                total_batches += len(batch)

                logger.debug(f"Enviado hasta el momento total_batches: {total_batches}")

        except Exception as e:
            logger.error(f"Error en process_connection: {e}", exc_info=True)
        finally:
            movie_producer.close()
            actor_producer.close()
            rating_producer.close()


    def start(self):

        logger.info("Iniciando servidor decodificador...")
        max_retries = 5
        retry_delay = 2
        consumer_thread = threading.Thread(target=self.result_consumer.start_consuming)
        consumer_thread.daemon = True
        consumer_thread.start()

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

        try:
            while True:
                logger.info("Esperando conexiones en el puerto 50000...")
                client_socket, addr = self.csv_receiver.accept_connection()
                if client_socket:
                    logger.info(f"Nueva conexión aceptada desde {addr}")
                    thread = threading.Thread(
                        target=self.process_connection,
                        args=(client_socket,)
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
            #self.csv_receiver.close()
            self.shutdown_event.wait()

    def exit_gracefully(self, signum, frame):
        self.close()

    def wait_for_result(self, query_result):
        self.results_received += 1
        logger.info(f"[INFO] Resultado {self.results_received} recibido: {query_result}")

        client_id = query_result.get("client_id")
        if not client_id:
            logger.warning("Error getting client_id")
            return

        logger.info("Trying to get socket to send result for client_id: " + str(client_id))

        with self.client_sockets_lock:
            client_socket = self.client_sockets.get(client_id)

        if client_socket:
            try:
                result = str(query_result) + "\n"
                client_socket.sendall(result.encode("utf-8"))
                logger.info(f"Resultado enviado a cliente {client_id}")
            except Exception as e:
                logger.error(f"Error enviando resultado a cliente {client_id}: {e}")
        else:
            logger.warning(f"No se encontró socket para client_id: {client_id}")

        self.test_producer.enqueue(query_result)

    def close(self):
        logger.info(f"Closing all workers")
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

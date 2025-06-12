import json
import signal
import threading
import time
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.file_consuming.file_consuming import CSVReceiver
from middleware.producer.producer import Producer
from worker.worker import Worker


class ClientDecodifier(Worker):
    def __init__(self):
        super().__init__()
        self.csv_receiver = CSVReceiver()
        self.client_sockets = {}
        self.client_sockets_lock = threading.Lock()

        #self.test_producer = Producer("result_comparator")

        self.result_consumer = Consumer("result", _message_handler=self.wait_for_result)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.results_received_per_client = defaultdict(int)
        self.shutdown_event = threading.Event()

    def close_client_socket(self, client_id):
        with self.client_sockets_lock:
            client_socket = self.client_sockets.get(client_id)
            if client_socket:
                client_socket.close()
                self.client_sockets.pop(client_id)

    def process_connection(self, client_socket):
        movie_producer = Producer("movie_main_filter")
        actor_producer = Producer("credits")
        rating_producer = Producer("ratings")
        try:
            self.logger.info("Iniciando procesamiento de nueva conexión")
            generated = self.csv_receiver.process_connection(client_socket)
            last_metadata = None
            total_batches = 0
            for client_id, batch, is_last, metadata in generated:
                with self.client_sockets_lock:
                    if client_id not in self.client_sockets:
                        self.client_sockets[client_id] = client_socket
                        self.logger.info("Socket: " + str(client_socket) + f" successfully added to client_sockets con client_id={client_id}")

                if metadata.type == "movie":
                    producer = movie_producer
                elif metadata.type == "credit":
                    producer = actor_producer
                elif metadata.type == "rating":
                    producer = rating_producer
                else:
                    self.logger.error(f"Metadata no reconocido")
                    return
                if last_metadata != metadata.type:
                    total_batches = 0
                last_metadata = metadata.type
                if not producer:
                    self.logger.error(f"Tipo de archivo no válido: {metadata.type}")
                    continue

                self.logger.debug(f"Preparando mensaje para enviar batch de tipo {metadata.type} con {len(batch)} líneas")
                
                message = {
                    "type": metadata.type,
                    "cola": batch,
                    "batch_size": len(batch),
                    "total_batches": total_batches + len(batch) if is_last else 0,
                    "client_id": client_id
                }

                self.logger.debug(f"Enviando {metadata.type} de {client_id}")

                if is_last:
                    self.logger.info(f"Enviando ultimo batch de {metadata.type} de {client_id}")

                if not self.send(message, producer):
                    self.logger.error(f"[ERROR] Falló el envío del batch {total_batches + 1} a RabbitMQ")
                
                total_batches += len(batch)

                self.logger.debug(f"Enviado hasta el momento total_batches: {total_batches}")

        except Exception as e:
            self.logger.error(f"Error en process_connection: {e}", exc_info=True)
        finally:
            movie_producer.close()
            actor_producer.close()
            rating_producer.close()


    def start(self):

        self.logger.info("Iniciando servidor decodificador...")
        max_retries = 5
        retry_delay = 2
        consumer_thread = threading.Thread(target=self.result_consumer.start_consuming)
        consumer_thread.daemon = True
        consumer_thread.start()

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Intento {attempt + 1} de iniciar el servidor")
                if self.csv_receiver.start_server():
                    self.logger.info("Servidor iniciado exitosamente")
                    break
                else:
                    raise Exception("No se pudo iniciar el servidor")
            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Error iniciando servidor: {e}. Reintentando en {retry_delay} segundos...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("No se pudo iniciar el servidor después de varios intentos")
                    return

        try:
            while True:
                self.logger.info("Esperando conexiones en el puerto 50000...")
                client_socket, addr = self.csv_receiver.accept_connection()
                if client_socket:
                    self.logger.info(f"Nueva conexión aceptada desde {addr}")
                    thread = threading.Thread(
                        target=self.process_connection,
                        args=(client_socket,)
                    )
                    thread.daemon = True
                    thread.start()
                else:
                    self.logger.error("Error aceptando conexión")

        except KeyboardInterrupt:
            self.logger.info("Cerrando servidor...")
        except Exception as e:
            self.logger.error(f"Error inesperado: {e}")
        finally:
            #self.csv_receiver.close()
            self.shutdown_event.wait()

    def exit_gracefully(self, signum, frame):
        self.close()

    def wait_for_result(self, query_result):
        client_id = query_result.get("client_id")
        self.results_received_per_client[client_id] += 1
        self.logger.info(f"[INFO] Resultado {self.results_received_per_client[client_id]} recibido: {query_result} de cliente {client_id}")

        if not client_id:
            self.logger.warning("Error getting client_id")
            return

        self.logger.info("Trying to get socket to send result for client_id: " + str(client_id))

        with self.client_sockets_lock:
            client_socket = self.client_sockets.get(client_id)

        if client_socket:
            try:
                result = json.dumps(query_result) + "\n"
                client_socket.sendall(result.encode("utf-8"))
                self.logger.info(f"Resultado enviado a cliente {client_id}")
                if self.results_received_per_client[client_id] == 5:
                    self.logger.info(f"se cierra connexion con cliente {client_id}")
                    self.close_client_socket(client_id)
            except Exception as e:
                self.logger.error(f"Error enviando resultado a cliente {client_id}: {e}")
        else:
            self.logger.warning(f"No se encontró socket para client_id: {client_id}")

        #self.test_producer.enqueue(query_result)

    def close(self):
        self.logger.info(f"Closing all workers")
        self.result_consumer.close()

    def send(self, message: dict, producer) -> bool:
        try:
            return producer.enqueue(message)
        except Exception as e:
            self.logger.error(f"[ERROR] Error al enviar mensaje: {e}")
            return False


if __name__ == '__main__':
    decodifier = ClientDecodifier()
    decodifier.start()

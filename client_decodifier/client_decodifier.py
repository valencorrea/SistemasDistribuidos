import json
import random
import signal
import string
import threading
import time
import uuid
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
        metadata = None
        client_id = None

        producers = {
            "movie": Producer("movie_main_filter"),
            "credit": Producer("credits"),
            "rating": Producer("ratings"),
        }
        last_metadata = None
        total_batches = 0
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

                if last_metadata != metadata.type:
                    total_batches = 0
                last_metadata = metadata.type

                self.logger.debug(f"Preparando mensaje para enviar batch de tipo {metadata.type} con {len(batch)} líneas")
                
                message = {
                    "type": metadata.type,
                    "cola": batch,
                    "batch_size": len(batch),
                    "client_id": client_id,
                    "batch_id": self.generate_batch_id(client_id, metadata.type, total_batches, is_last),
                }
                if is_last:
                    message["total_batches"] = total_batches + len(batch)

                self.logger.debug(f"Enviando {metadata.type} de {client_id} con batch_id {message['batch_id']}")

                if is_last:
                    self.logger.info(f"Enviando ultimo batch de {metadata.type} de {client_id}")

                try:
                    producers[metadata.type].enqueue(message)
                except Exception as e:
                    self.logger.error(f"[ERROR] Falló el envío del batch {total_batches + 1} a RabbitMQ")
                
                total_batches += len(batch)

                self.logger.debug(f"Enviado hasta el momento total_batches: {total_batches}")

        except Exception as e:
            self.logger.error(f"Error en process_connection: {e}", exc_info=True)
            self.logger.error(
                f"Error al procesar la conexión, limpiando cliente. Ultima metadata: {last_metadata}, batches enviados: {total_batches}")
            self.send_poisoned_messages(producers, client_id, metadata.type, total_batches)

        self.logger.info(f"Terminando thread de cliente")
        for producer in producers.values():
            producer.close()

    def send_poisoned_messages(self, producers, client_id, metadata_type, total_batches):
        # Este metodo es para enviar un mensaje final falso para forzar la limpieza de los recursos de un cliente
        if not metadata_type or not client_id:
            self.logger.info("Parece no haberse enviado nada todavia, no se enviará mensaje final")
            return
        if metadata_type == "movie":
            if total_batches == 0:
                self.logger.info(f"No se envio nada con movies, no es necesario enviar mensaje final")
                return
            self.logger.info(f"Enviando mensaje final a cliente para metadata {metadata_type} con {total_batches} batches")
            final_message = {
                "type": "movie",
                "batch_size": 0,
                "total_batches": total_batches,
                "client_id": client_id,
                "batch_id": self.generate_batch_id(client_id, "movie", total_batches, is_poison = True),
                "is_final": True,
            }
            producers["movie"].enqueue(final_message)
        if metadata_type == "credit" or metadata_type == "movie":
            current_amount = total_batches if metadata_type is "credit" else 0
            self.logger.info(f"Enviando mensaje final a cliente para metadata credit con {current_amount} batches")
            final_message = {
                "type": "credit",
                "batch_size": 0,
                "total_batches": current_amount,
                "client_id": client_id,
                "batch_id": self.generate_batch_id(client_id, "credit", current_amount, is_poison = True),
                "is_final": True,
            }
            producers["credit"].enqueue(final_message)
        if metadata_type is not None:
            current_amount = total_batches if metadata_type is "rating" else 0
            self.logger.info(f"Enviando mensaje final a cliente para metadata rating con {current_amount} batches")
            final_message = {
                "type": "rating",
                "batch_size": 0,
                "total_batches": current_amount,
                "client_id": client_id,
                "batch_id": self.generate_batch_id(client_id, "rating", current_amount, is_poison = True),
                "is_final": True,
            }
            producers["rating"].enqueue(final_message)

    @staticmethod
    def generate_batch_id(client_id, metadata_type, order_number, is_total: bool = False, is_poison: bool = False):
        rand_str = ''.join(random.choices(string.ascii_uppercase, k=4))
        order_str = f"{order_number:04d}"
        return f"{client_id}-{metadata_type}-{order_str}-{'Y' if is_total else 'P' if is_poison else 'N'}-{rand_str}"

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


if __name__ == '__main__':
    decodifier = ClientDecodifier()
    decodifier.start()

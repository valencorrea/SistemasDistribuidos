from collections import defaultdict
import logging
import os
import json
import threading
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from middleware.producer.publisher import Publisher
from middleware.tcp_protocol.tcp_protocol import TCPServer
from worker.abstractaggregator.abstractaggregator import AbstractAggregator


class Aggregator(AbstractAggregator):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.DEBUG,
            datefmt='%H:%M:%S')
        self.control_received_batches_per_client = defaultdict(int)
        self.batches_by_joiner = defaultdict(set)
        self.tcp_host = os.getenv("AGGREGATOR_HOST", "best_and_worst_ratings_aggregator")
        self.tcp_port = int(os.getenv("AGGREGATOR_PORT", 60002))
        self.joiner_control_publisher = Publisher("joiner_control_ratings")
        self.processed_batches_map = defaultdict(set)
        self.control_log_name = "_ratings_control.log"
        self.batch_to_joiner = {}
        self.recover_control_messages()
        super().__init__()
        self.tcp_server = TCPServer(self.tcp_host, self.tcp_port, self._handle_tcp_message)
        self.logger.info(f"TCP Server inicializado en {self.tcp_host}:{self.tcp_port}")

    def create_consumer(self):
        return Consumer("best_and_worst_ratings_partial_result",
                        _message_handler=self.handle_message_joiner_aggregator)

    def create_producer(self):
        return Producer("result")

    def handle_message_joiner_aggregator(self, message):
        type_of_message = message.get("type")
        if type_of_message == "query_3_arg_2000_ratings":
            self.handle_message(message)
        elif type_of_message == "control":
            self.handle_control_message(message)
        else:
            self.logger.error(f"Tipo de mensaje desconocido: {type_of_message}. Mensaje: {message}")

    def process_message(self, client_id, message):
        ratings = message.get("ratings")
        partial_result = {}
        for movie_id, data in ratings.items():
            if movie_id in partial_result:
                partial_result[movie_id]["rating_sum"] += float(data.get("rating_sum", 0))
                partial_result[movie_id]["votes"] += int(data.get("votes", 0))
            else:
                partial_result[movie_id] = {
                    "title": data.get("title", ""),
                    "rating_sum": float(data.get("rating_sum", 0)),
                    "votes": int(data.get("votes", 0))
                }
        return partial_result

    def aggregate_message(self, client_id, actors):
        if client_id not in self.results:
            self.results[client_id] = {}
            self.logger.info(f"Creando un nuevo registro para el cliente {client_id}.")
        for movie_id, data in actors.items():
            if movie_id in self.results[client_id]:
                self.logger.info(f"Agregando rating para la película {movie_id} del cliente {client_id}")
                self.results[client_id][movie_id]["rating_sum"] += float(data.get("rating_sum", 0))
                self.results[client_id][movie_id]["votes"] += int(data.get("votes", 0))
            else:
                self.results[client_id][movie_id] = {
                    "title": data.get("title", ""),
                    "rating_sum": float(data.get("rating_sum", 0)),
                    "votes": int(data.get("votes", 0))
                }

    def create_final_result(self, client_id):
        result = self.obtain_result(client_id)
        return {
            "result_number": 3,
            "type": "best_and_worst_movies",
            "actors": result,
            "client_id": client_id
        }

    def obtain_result(self, client_id):
        max_rating = float('-inf')
        min_rating = float('inf')
        best_movie = None
        worst_movie = None

        for movie_id, data in self.results[client_id].items():
            if data["votes"] > 0:
                avg_rating = data["rating_sum"] / data["votes"]
                movie_data = {
                    "id": movie_id,
                    "title": data["title"],
                    "rating": avg_rating,
                }

                # Actualizar el mejor rating
                if avg_rating > max_rating:
                    max_rating = avg_rating
                    best_movie = movie_data

                # Actualizar el peor rating
                if avg_rating < min_rating:
                    min_rating = avg_rating
                    worst_movie = movie_data

        return {
            "best": best_movie,
            "best_rating": max_rating,
            "worst": worst_movie,
            "min_rating": min_rating,
        }

    def _handle_tcp_message(self, msg, addr, client_socket):
        try:
            data_json = json.loads(msg)
            message_type = data_json.get("type")
            if message_type == "control": #mensaje de control para guardar el batch_id y el joiner_instance_id
                batch_id = data_json.get("batch_id")
                joiner_instance_id = data_json.get("joiner_instance_id")
                client_id = data_json.get("client_id")
                joiner_instance_id_from_dic = self.batch_to_joiner.get(batch_id, None)
                if joiner_instance_id_from_dic is None:
                    self.batch_to_joiner[batch_id] = joiner_instance_id

                self.handle_control_message(data_json)

            elif message_type == "batch_processed": #consulta del joiner cuando arranca para saber si ya proceso el batch
                self.logger.info(f"Respuesta recibida del joiner para batch {data_json.get('batch_id')}: {self.batch_to_joiner.get(data_json.get('batch_id'), '-1')}")
                joiner_instance_id = self.batch_to_joiner.get(data_json.get("batch_id"), '-1')
                client_socket.send(json.dumps({
                    "type": "batch_processed",
                    "joiner_instance_id": joiner_instance_id
                }).encode('utf-8') + b'\n')
            else:
                self.logger.warning(f"[TCP] Tipo de mensaje desconocido: {message_type}")
                
        except Exception as e:
            self.logger.error(f"[TCP] Error procesando mensaje recibido: {e}")

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.tcp_server.stop()
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

    def handle_control_message(self, message):
        if message.get("is_final", False):
            client_id = message.get("client_id")
            if client_id:
                self.logger.info(f"Recibido mensaje envenenado para cliente {client_id}, limpiando datos...")
                control_log_filename = f"{client_id}{self.control_log_name}"
                if os.path.exists(control_log_filename):
                    os.remove(control_log_filename)
                self.results.pop(client_id, None)
                self.total_batches_per_client.pop(client_id, None)
                self.received_batches_per_client.pop(client_id, None)
                self.control_received_batches_per_client.pop(client_id, None)
                batch_id = message.get("batch_id")
                if batch_id and self.consumer:
                    self.consumer.ack(batch_id)
                self.logger.info(f"Datos del cliente {client_id} limpiados, mensaje envenenado confirmado")
            return

        total_batches = message.get("total_batches", None)
        batch_size = message.get("batch_size")
        joiner_id = message.get("joiner_id")
        batch_id = message.get("batch_id")
        client_id = message.get("client_id")

        self.persist_control_message(client_id, batch_id, joiner_id, batch_size, total_batches)
        self.control_received_batches_per_client[client_id] = self.control_received_batches_per_client.get(client_id,
                                                                                                           0) + batch_size
        self.batches_by_joiner[joiner_id].add(batch_id)

        if batch_id and joiner_id:
            self.batch_to_joiner[batch_id] = joiner_id
            self.logger.info(f"[Guardado en batch_to_joiner: {batch_id} -> {joiner_id}")
    
        if total_batches is not None:
            self.total_batches_per_client[client_id] = total_batches
            self.logger.info(f"Se actualiza la cantidad total de batches:"
                             f"{self.total_batches_per_client[client_id]} para el cliente {client_id}.")
        self.consumer.ack(batch_id)
        #Liberar el lock
        if client_id not in self.results:
            self.results[client_id] = {}
        total = self.total_batches_per_client.get(client_id, None)
        received = self.control_received_batches_per_client.get(client_id, 0)
        if total is not None and total <= received:
            self.logger.info(f"Se proceso el cliente {client_id}: ({received}/{total}), enviando request de resultados parciales.")
            self.joiner_control_publisher.enqueue({"client_id": client_id})
            control_log_filename = f"{client_id}{self.control_log_name}"
            if os.path.exists(control_log_filename):
                os.remove(control_log_filename)

    def start(self):
        self.logger.info("Iniciando agregador")
        
        tcp_thread = threading.Thread(target=self.tcp_server.start, daemon=True)
        tcp_thread.start()
        self.logger.info("TCP Server iniciado en thread separado")
        super().start()

    def persist_control_message(self, client_id, batch_id, joiner_id, batch_size, total_batches):
        log_file = f"{client_id}{self.control_log_name}"
        payload = {"batch_id": batch_id, "joiner_id": joiner_id, "batch_size": batch_size}
        if total_batches:
            payload["total_batches"] = total_batches
        self.persist_entry(batch_id, log_file, payload)

    def recover_control_messages(self):
        self.logger.info("Recuperando mensajes de control de batches desde el directorio de control.")

        for filename in os.listdir():
            if not filename.endswith(self.control_log_name):
                self.logger.info(f"El archivo {filename} no es un archivo de control, se omite.")
                continue

            client_id = filename.replace(self.control_log_name, "")
            self.logger.info(f"Recuperando mensajes de control para el cliente {client_id} desde {filename}")
            try:
                with open(filename, "r") as f:
                    for line in f:
                        parts = line.strip().split(";", 2)

                        if parts[0] == "BEGIN_TRANSACTION" and len(parts) == 3:
                            in_transaction = True
                            current_batch_id = parts[1]
                            current_payload = json.loads(parts[2])
                            self.logger.info(f"Se encontró una transacción para el id {current_batch_id}.")

                        elif parts[0] == "END_TRANSACTION" and len(parts) == 2 and in_transaction:
                            batch_id = parts[1]
                            if batch_id != current_batch_id:
                                self.logger.error(
                                    f"Mismatch de batch_id en transacción: {batch_id} != {current_batch_id}")
                                continue

                            joiner_id = current_payload.get("joiner_id")
                            batch_size = current_payload.get("batch_size", 0)
                            total_batches = current_payload.get("total_batches", None)
                            self.batches_by_joiner[joiner_id].add(batch_id)
                            self.batch_to_joiner[batch_id] = joiner_id
                            self.logger.info(f"Se recuperó el control batch_id {batch_id} para el joiner {joiner_id}.")
                            self.control_received_batches_per_client[
                                client_id] = self.control_received_batches_per_client.get(client_id, 0) + batch_size
                            self.logger.info(f"Se recupera la cantidad total de batches: "
                                                 f"{self.control_received_batches_per_client[client_id]} para el cliente {client_id}.")
                            if total_batches:
                                self.total_batches_per_client[client_id] = total_batches

                            # Reset
                            in_transaction = False
                            current_batch_id = None
                            current_payload = None
            except json.JSONDecodeError as e:
                self.logger.exception(f"Error decodificando JSON de batch {current_batch_id}: {e}")
            except Exception as e:
                self.logger.exception(f"Error al intentar recuperar el archivo de log {current_batch_id}: {e}")
                exit(1)


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()

import json
import os
import random
import string
import logging
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from utils.parsers.ratings_parser import convert_data_for_rating_joiner
from worker.abstractaggregator.abstractaggregator import AbstractAggregator
from middleware.tcp_protocol.tcp_protocol import TCPClient

PENDING_MESSAGES = "/root/files/ratings_pending.jsonl"


class RatingsJoiner(AbstractAggregator):
    def create_consumer(self):
        return Consumer("ratings", _message_handler=self.handle_message)

    def create_producer(self):
        return Producer("best_and_worst_ratings_partial_result")

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.DEBUG,
            datefmt='%H:%M:%S')

        self.joiner_instance_id = os.environ.get("JOINER_INSTANCE_ID", "joiner_ratings")
        self.has_recovered_at_least_once = False
        self.movies_name = "_ratings_movies.json"
        self.pending_file = "_ratings_pending.json"
        self.movies = {}

        aggregator_host = os.getenv("AGGREGATOR_HOST", "best_and_worst_ratings_aggregator")
        aggregator_port = int(os.getenv("AGGREGATOR_PORT", 60002))
        self.tcp_client = TCPClient(aggregator_host, aggregator_port)
        self.results = {}
        self.recover_movies()
        super().__init__(self.results)
    
        self.movies_consumer = Subscriber("20_century_arg_result",
                                          message_handler=self.handle_movies_message)
        self.ratings_producer = Producer(queue_name="ratings", queue_type="direct")
        self.control_consumer = Subscriber("joiner_control_ratings", message_handler=self.handle_control_message)

        if self.has_recovered_at_least_once and self.consumer:
            self.consumer.start()

    def process_message(self, client_id, message):
        if client_id not in self.results:
            self.add_to_pending(client_id, message)
            return None
        ratings = convert_data_for_rating_joiner(message)
        self.logger.info(f"Ratings convertidos. Total recibido: {len(ratings)}")
        partial_result = defaultdict(dict)
        self.logger.info(f"Partial result convertidos. Total recibido: {len(self.results[client_id].keys())}")
        for rating in ratings:
            if not isinstance(rating, dict):
                self.logger.error(f"Rating no es un diccionario: {rating}")
                continue
            movie_id = rating.get("movieId")
            if int(movie_id) in self.results[client_id].keys():
                if movie_id not in partial_result:
                    partial_result[movie_id] = {
                        "rating_sum": float(rating.get("rating", 0)),
                        "votes": 1
                    }
                else:
                    partial_result[movie_id]["rating_sum"] += float(rating.get("rating", 0))
                    partial_result[movie_id]["votes"] += 1
        return partial_result

    def add_to_pending(self, client_id, message):
        batch_id = message.get("batch_id")
        pending_file = f"{client_id}{self.pending_file}"
        with open(pending_file, "a") as f:
            f.write(f"BEGIN_TRANSACTION;{batch_id};{json.dumps(message)}\n")
            f.write(f"END_TRANSACTION;{batch_id}\n")
            f.flush()
            os.fsync(f.fileno())

    def aggregate_message(self, client_id, result):
        self.logger.info(f"Agregando resultados para el cliente {client_id}. Resultados: {result}")
        if client_id not in self.results:
            self.results[client_id] = result
        else:

            for movie_id, data in result.items():
                if movie_id not in self.results[client_id]:
                    self.results[client_id][movie_id] = {
                        "rating_sum": data["rating_sum"],
                        "votes": data["votes"]
                    }
                else:
                    self.results[client_id][movie_id]["rating_sum"] += data["rating_sum"]
                    self.results[client_id][movie_id]["votes"] += data["votes"]

    def check_if_its_completed(self, client_id):
        pass

    def create_final_result(self, client_id):
        return {
            "type": "query_3_arg_2000_ratings",
            "ratings": self.get_result(client_id),
            "client_id": client_id,
            "batch_size": self.received_batches_per_client[client_id],
            "batch_id": self.generate_batch_id(client_id, self.joiner_instance_id)
        }

    def handle_control_message(self, message):
        self.logger.info(f"Mensaje de control recibido: {message}")
        client_id = message["client_id"]
        result_message = self.create_final_result(client_id)
        if self.producer:
            self.producer.enqueue(result_message)
        self.logger.info(f"Resultado enviado {result_message}.")
        self.clean_client(client_id)

    def clean_client(self, client_id):
        try:
            movies_file = f"{client_id}{self.movies_name}"
            if os.path.exists(movies_file):
                os.remove(movies_file)
            pending_file = f"{client_id}{self.pending_file}"
            if os.path.exists(pending_file):
                os.remove(pending_file)
            results_file = f"{client_id}{self.results_log_name}"
            if os.path.exists(results_file):
                os.remove(results_file)
            self.results.pop(client_id)
            self.total_batches_per_client.pop(client_id)
            self.received_batches_per_client.pop(client_id)
        except Exception as e:
            self.logger.error(f"Error al limpiar cliente {client_id}: {e}")

    @staticmethod
    def generate_batch_id(client_id, joiner_id):
        rand_str = ''.join(random.choices(string.ascii_uppercase, k=4))
        return f"credits-{client_id}-{joiner_id}-{rand_str}"

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.movies_consumer.close()
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()
            if self.shutdown_consumer:
                self.shutdown_consumer.close()
            self.control_consumer.close()
            self.ratings_producer.close()
            if self.tcp_client:
                self.tcp_client.close()
        except Exception as e:
            self.logger.exception(f"❌ Error al cerrar conexiones: {e}")

    def send_batch_processed(self, client_id, batch_id, batch_size, total_batches):
        control_message = {
            "type": "control",
            "client_id": client_id,
            "batch_id": batch_id,
            "batch_size": batch_size,
            "joiner_id": self.joiner_instance_id,
        }
        if total_batches is not None:
            control_message["total_batches"] = total_batches

        if self.producer:
            self.producer.enqueue(control_message)
        self.logger.info(f"Control enviado al aggregator: {control_message}")

    def get_result(self, client_id):
        movies_with_ratings = {}
        for movie_id, data in self.results[client_id].items():
            if data["votes"] > 0:
                movies_with_ratings[movie_id] = data
        self.logger.info(f"Obtenidas {len(movies_with_ratings.keys())} peliculas con al menos un rating")
        return movies_with_ratings

    def handle_movies_message(self, message):
        self.logger.info(f"Mensaje de movies recibido - cliente: " + str(message.get("client_id")))

        if message.get("type") != "20_century_arg_total_result":
            self.logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")
            return

        client_id = message.get("client_id")
        movies = message.get("movies")

        if not client_id:
            self.logger.error("Mensaje sin client_id. Se descarta.")
            return

        if not isinstance(movies, list):
            self.logger.error("Campo 'movies' no es una lista válida.")
            return

        try:
            self.logger.info(f"Se van a guardar {len(movies)} películas para el cliente {client_id}")
            self.persist_movies(client_id, movies)
            self.set_movies_for_client(client_id, movies)

            for filename in os.listdir():
                if not filename.endswith(self.pending_file):
                    self.logger.info(f"Archivo {filename} no es un archivo de mensajes pendientes, se omite.")
                    continue
                client_id = filename.replace("%s" % self.pending_file, "")
                self.logger.info(f"Recuperando mensajes pendientes para cliente {client_id} desde {filename}")
                in_transaction = False
                current_batch_id = None
                current_payload = None

                with open(filename, "r") as f:
                    try:
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
                                if batch_id in self.processed_batch_ids:
                                    self.logger.info(f"Batch {batch_id} ya procesado, se omite.")
                                    continue

                                self.ratings_producer.enqueue(current_payload)
                                in_transaction = False
                                current_batch_id = None
                                current_payload = None
                    except json.JSONDecodeError as e:
                        self.logger.exception(f"Error decodificando JSON de batch {current_batch_id}: {e}")
                    except Exception as e:
                        self.logger.exception(f"Error al intentar recuperar el archivo de log {current_batch_id}: {e}")
                        exit(1)
                os.remove(filename)

            self.recheck_if_some_client_is_completed_after_restart()
            self.logger.info(f"{len(self.results[client_id])} películas guardadas para {client_id}")
            if self.consumer and not self.consumer.is_alive():
                self.consumer.start()
                self.logger.info("Thread de consumo de ratings empezado")
            else:
                self.logger.info("Thread de consumo de ratings no empezado, ya existe uno")

            self.recheck_if_some_client_is_completed_after_restart()

        except Exception as e:
            self.logger.error(f"Error al procesar mensaje de películas: {e}", exc_info=True)
            self.close()

    def set_movies_for_client(self, client_id, movies):
        self.results[client_id] = {}
        for movie in movies:
            if not isinstance(movie, dict):
                self.logger.warning(f"Pelicula no es un diccionario. Pelicula: {movie}")
                continue
            movie_id = movie.get("id")
            if not movie_id:
                self.logger.warning(f"Pelicula sin ID: {movie}")
                continue

            self.results[client_id][int(movie_id)] = {
                "title": movie.get("title", ""),
                "rating_sum": 0,
                "votes": 0
            }

    def persist_movies(self, client_id, movies):
        try:
            self.logger.info(f"Se va a intentar persistir el archivo de movies para el cliente {client_id}")
            movies_file = f"{client_id}{self.movies_name}"
            with open(movies_file, "w") as f:
                f.write(f"BEGIN_TRANSACTION;{json.dumps(movies)}\n")
                f.write(f"END_TRANSACTION;\n")
                f.flush()
                os.fsync(f.fileno())
                self.logger.info(f"Se persistio el archivo de movies para el cliente {client_id}")
        except Exception:
            self.logger.exception(f"Error al intentar persistir el archivo de movies para el cliente {client_id}")

    def recover_movies(self):
        for filename in os.listdir():
            if not filename.endswith(self.movies_name):
                self.logger.info(f"Archivo {filename} no es un archivo de movies, se omite.")
                continue

            client_id = filename.replace(f"{self.movies_name}", "")
            self.logger.info(f"Recuperando movies para cliente {client_id} desde {filename}")

            try:
                with open(filename, "r") as f:
                    lines = [line.strip() for line in f.readlines()]

                    if len(lines) != 2 or not lines[0].startswith("BEGIN_TRANSACTION;") or lines[
                        1] != "END_TRANSACTION;":
                        self.logger.error(f"Formato inválido en archivo {filename}. Se omite.")
                        continue

                    raw_json = lines[0][len("BEGIN_TRANSACTION;"):]
                    movies = json.loads(raw_json)
                    self.set_movies_for_client(client_id, movies)
                    self.has_recovered_at_least_once = True
                    self.logger.info(
                        f"Películas recuperadas para cliente {client_id}: {len(self.results[client_id])} items.")

            except json.JSONDecodeError as e:
                self.logger.exception(f"Error decodificando JSON en archivo {filename}: {e}")
            except Exception as e:
                self.logger.exception(f"Error al intentar recuperar películas desde archivo {filename}: {e}")

    def start(self):
        self.logger.debug("Iniciando joiner de ratings")
        try:
            self.movies_consumer.start()
            self.control_consumer.start()
            self.shutdown_event.wait()
        finally:
            self.close()

    def format_message(self, message):
        if isinstance(message, dict):
            return json.dumps(message) + '\n'
        else:
            return str(message) + '\n'

    def handle_message(self, message):
        if message.get("is_final", False):
            client_id = message.get("client_id")
            if client_id:
                self.logger.info(f"Recibido mensaje envenenado para cliente {client_id}, limpiando datos...")
                self.clean_client(client_id)
                
                # Reenviar mensaje envenenado al aggregator para que también limpie
                poisoned_control_message = {
                    "type": "control",
                    "client_id": client_id,
                    "batch_id": message.get("batch_id"),
                    "batch_size": 0,
                    "joiner_id": self.joiner_instance_id,
                    "is_final": True
                }
                if self.producer:
                    self.producer.enqueue(poisoned_control_message)
                    self.logger.info(f"Mensaje envenenado reenviado al aggregator para cliente {client_id}")
                
                # Hacer ACK del mensaje envenenado para que no se reprocese
                batch_id = message.get("batch_id")
                if batch_id and self.consumer:
                    self.consumer.ack(batch_id)
                self.logger.info(f"Datos del cliente {client_id} limpiados, mensaje envenenado confirmado")
            return

        batch_id = message.get("batch_id")
        client_id = message.get("client_id")
        
        control_message = {
            "type": "batch_processed",
            "batch_id": batch_id,
            "client_id": client_id,
        }
        
        formatted_message = self.format_message(control_message)
        self.logger.info(f"Enviando mensaje de batch_processed al aggregator: {control_message}")
        processing_status = self.tcp_client.send_with_response(formatted_message, self._handle_batch_processed)
        
        if processing_status is True:
            if self.consumer:
                self.consumer.ack(batch_id)
        elif processing_status is False:
            super().handle_message(message)
        elif processing_status is None:
            self.logger.warning(f"⚠️ Batch {batch_id} fallo al preguntar al aggregator si ya lo proceso alguien")
        else:
            self.logger.error(f"❌ Estado de procesamiento inesperado: {processing_status}")

    def _handle_batch_processed(self, response):
        self.logger.info(f"Respuesta recibida del aggregator: {response}")
        joiner_instance_id = response.get("joiner_instance_id", '-1')
        return joiner_instance_id != '-1'
    
    def _handle_batch_processed_for_recover(self, response):
        joiner_instance_id = response.get("joiner_instance_id", '-1')
        return joiner_instance_id == self.joiner_instance_id

    def should_resolve_unfinished_transaction(self, batch_id):
        control_message = {
            "type": "batch_processed",
            "batch_id": batch_id,
        }        
        formatted_message = self.format_message(control_message)
        self.logger.info(f"Enviando mensaje de should_resolve_unfinished_transaction al aggregator: {batch_id}")
        response = self.tcp_client.send_with_response(formatted_message, self._handle_batch_processed_for_recover)
        if response is None:
            self.logger.warning(f"⚠️ Batch {batch_id} fallo al preguntar al aggregator si ya lo procese yo")
            return False
        return response


if __name__ == '__main__':
    worker = RatingsJoiner()
    worker.start()

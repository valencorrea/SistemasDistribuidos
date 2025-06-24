import json
import os
import random
import string
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from utils.parsers.ratings_parser import convert_data_for_rating_joiner
from worker.abstractaggregator.abstractaggregator import AbstractAggregator

PENDING_MESSAGES = "/root/files/ratings_pending.jsonl"


class RatingsJoiner(AbstractAggregator):
    def __init__(self):
        super().__init__()
        self.has_recovered_at_least_one = False
        self.movies_name = "_credits_movies.json"
        self.joiner_instance_id = os.environ.get("JOINER_INSTANCE_ID", "joiner_credits")
        self.movies = {}
        self.recover_movies()
        self.movies_consumer = Subscriber("20_century_arg_result",
                                          message_handler=self.handle_movies_message)
        self.ratings_producer = Producer(queue_name="ratings", queue_type="direct")
        self.control_consumer = Subscriber("joiner_control_ratings", message_handler=self.handle_control_message)
        if self.has_recovered_at_least_one:
            self.consumer.start()

    def create_consumer(self):
        return Consumer("ratings", _message_handler=self.handle_message)

    def create_producer(self):
        return Producer("best_and_worst_ratings_partial_result")

    def process_message(self, client_id, message):
        # if client_id not in self.results:
        #     os.makedirs(os.path.dirname(PENDING_MESSAGES), exist_ok=True)
        #     self.logger.info("client id " + client_id + " not ready for credits file. Saving locally")
        #     with open(PENDING_MESSAGES, "a") as f:
        #         f.write(json.dumps(message) + "\n")
        #     return {}  # TODO validar este caso
        ratings = convert_data_for_rating_joiner(message)
        self.logger.info(f"Ratings convertidos. Total recibido: {len(ratings)}")
        partial_result = defaultdict(dict)
        self.logger.info(f"Partial result convertidos. Total recibido: {len(self.results[client_id].keys())}")
        for rating in ratings:
            if not isinstance(rating, dict):
                self.logger.error(f"Rating no es un diccionario: {rating}")
                continue
            movie_id = rating.get("movieId")
            if int(movie_id) in self.results[client_id].keys() or str(movie_id) in self.results[client_id].keys():
                if movie_id not in partial_result:
                    partial_result[movie_id] = {
                        "rating_sum": float(rating.get("rating", 0)),
                        "votes": 1
                    }
                else:
                    partial_result[movie_id]["rating_sum"] += float(rating.get("rating", 0))
                    partial_result[movie_id]["votes"] += 1
        return partial_result

    def aggregate_message(self, client_id, result):
        self.logger.info(f"Agregando resultados para el cliente {client_id}. Resultados: {result}")
        if client_id not in self.results:
            self.results[client_id] = result
        else:
            for movie_id, data in result.items():
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
        self.producer.enqueue(result_message)
        self.logger.info(f"Resultado enviado {result_message}.")
        self.results.pop(client_id)

    @staticmethod
    def generate_batch_id(client_id, joiner_id):
        rand_str = ''.join(random.choices(string.ascii_uppercase, k=4))
        return f"credits-{client_id}-{joiner_id}-{rand_str}"

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.movies_consumer.close()
            self.consumer.close()
            self.producer.close()
            self.producer.close()
            self.shutdown_consumer.close()
            self.control_consumer.close()
        except Exception as e:
            self.logger.exception(f"❌ Error al cerrar conexiones: {e}")

    def send_batch_processed(self, client_id, batch_id, batch_size, total_batches):
        # TODO cuando me recupero, tengo que enviar el ultimo batch_id que persisti por si las dudas
        control_message = {
            "type": "control",
            "client_id": client_id,
            "batch_id": batch_id,
            "batch_size": batch_size,
            "joiner_id": self.joiner_instance_id,
        }
        if client_id in self.total_batches_per_client:
            control_message["total_batches"] = self.total_batches_per_client[client_id]
        # Enviar por tcp
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

            if os.path.exists(PENDING_MESSAGES):
                temp_path = PENDING_MESSAGES + ".tmp"
                with open(PENDING_MESSAGES, "r") as infile, open(temp_path, "w") as outfile:
                    for line in infile:
                        try:
                            pending_msg = json.loads(line)
                        except json.JSONDecodeError:
                            self.logger.warning("Línea inválida en archivo pendiente.")
                            continue

                        if pending_msg.get("client_id") == client_id:
                            self.logger.info(f"Reprocesando mensaje pendiente para cliente {client_id}")
                            self.ratings_producer.enqueue(pending_msg)
                        else:
                            outfile.write(line)

                os.replace(temp_path, PENDING_MESSAGES)

            self.logger.info(f"{len(self.results[client_id])} películas guardadas para {client_id}")
            if not self.consumer.is_alive():
                self.consumer.start()
                self.logger.info("Thread de consumo de ratings empezado")
            else:
                self.logger.info("Thread de consumo de ratings no empezado, ya existe uno")

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

            self.results[client_id][movie_id] = {
                "title": movie.get("title", ""),
                "rating_sum": 0,
                "votes": 0
            }

    def persist_movies(self, client_id, movies):
        # TODO hacer ack manual para Suscriber y ackearlo apenas se escriba en el archivo
        try:
            self.logger.info(f"Se va a intentar persistir el archivo de movies para el cliente {client_id}")
            movies_file = f"{client_id}{self.movies_name}"
            with open(movies_file, "w") as f:
                f.write(f"BEGIN_TRANSACTION;{json.dumps(movies)}\n")
                f.write(f"END_TRANSACTION;\n")
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

                    # Validamos que sea un archivo valido, sino nos caimos guardando el archivo
                    if len(lines) != 2 or not lines[0].startswith("BEGIN_TRANSACTION;") or lines[
                        1] != "END_TRANSACTION;":
                        self.logger.error(f"Formato inválido en archivo {filename}. Se omite.")
                        # TODO si es invalido borrar el archivo
                        continue

                    raw_json = lines[0][len("BEGIN_TRANSACTION;"):]
                    movies = json.loads(raw_json)
                    self.set_movies_for_client(client_id, movies)
                    self.has_recovered_at_least_one = True
                    self.logger.info(
                        f"Películas recuperadas para cliente {client_id}: {len(self.movies[client_id])} items.")

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


if __name__ == '__main__':
    worker = RatingsJoiner()
    worker.start()

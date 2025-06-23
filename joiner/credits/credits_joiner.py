import json
import os
import random
import string

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from utils.parsers.credits_parser import convert_data
from worker.abstractaggregator.abstractaggregator import AbstractAggregator

PENDING_MESSAGES = "/root/files/credits_pending.jsonl"


class CreditsJoiner(AbstractAggregator):
    def __init__(self):
        super().__init__()
        self.has_recovered_at_least_one = False
        self.movies_name = "_credits_movies.json"
        self.joiner_instance_id = os.environ.get("JOINER_INSTANCE_ID", "joiner_credits")
        self.movies = {}
        self.recover_movies()
        self.logger.info(f"Se finalizo la recuperacion de movies.")
        self.logger.info(f"Se inicializo como worker.")
        self.movies_consumer = Subscriber("20_century_arg_result",
                                          message_handler=self.handle_movies_message)
        self.credits_producer = Producer(queue_name="credits",queue_type="direct")
        self.control_consumer = Subscriber("joiner_control_credits", message_handler=self.handle_control_message)
        if self.has_recovered_at_least_one:
            self.consumer.start()

    def create_consumer(self):
        return Consumer("credits", _message_handler=self.handle_message)

    def create_producer(self):
        return Producer("top_10_actors_from_batch")

    def process_message(self, client_id, message):
        actors = convert_data(message)
        partial_result = {}
        for actor in actors:
            if actor.movie_id in self.movies.get(client_id, set()):
                actor_id = str(actor.id)
                actor_name = actor.name
                if actor_name == "Ricardo Darín":
                    self.logger.info(f"Actor {actor_name} encontrado en la película {actor.movie_id} del cliente {client_id}")
                if actor_id not in partial_result:
                    partial_result[actor_id] = {"name": actor_name, "count": 1}
                else:
                    partial_result[actor_id]["count"] += 1
        return partial_result

    def aggregate_message(self, client_id, result):
        if not self.results.get(client_id):
            self.results[client_id] = {}
        for actor_id, actor_data in result.items():
            actor_id = str(actor_id)
            if actor_id not in self.results[client_id]:
                self.results[client_id][actor_id] = {"name": actor_data["name"], "count": 0}
            self.results[client_id][actor_id]["count"] += actor_data["count"]

    def check_if_its_completed(self, client_id):
        pass

    def create_final_result(self, client_id):
        top_10 = self.get_result(client_id)
        result = {
            "type": "query_4_top_10_actores_credits",
            "actors": top_10,
            "client_id": client_id,
            "batch_size": self.received_batches_per_client[client_id],
            "batch_id": self.generate_batch_id(client_id, self.joiner_instance_id)
        }
        if client_id in self.total_batches_per_client:
            result["total_batches"] = self.total_batches_per_client[client_id]
        return result

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
            self.credits_producer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

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
        top_10 = sorted(self.results[client_id].items(), key=lambda item: item[1]["count"], reverse=True)
        self.logger.info("Top 10 actores con más contribuciones:")
        return top_10

    def handle_movies_message(self, message):
        print("MENSAJE DE MOVIES RECIBIDO")
        self.logger.info(f"Mensaje de movies recibido - cliente: " + str(message.get("client_id")))
        if message.get("type") == "20_century_arg_total_result":
            self.process_movie_message(message)
        else:
            self.logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")

    def process_movie_message(self, message):
        # TODO chequear que sea por cliente y no todo el archivo
        client_id = message.get("client_id")
        movies = [movie["id"] for movie in message.get("movies")]
        self.persist_movies(client_id, movies)
        self.movies[client_id] = movies
        self.logger.info(f"Obtenidas {message.get('total_movies')} películas")

        if os.path.exists(PENDING_MESSAGES):
            temp_path = PENDING_MESSAGES + ".tmp"
            with open(PENDING_MESSAGES, "r") as reading_file, open(temp_path, "w") as writing_file:
                for line in reading_file:
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        self.logger.warning("Invalid file line.")
                        continue

                    if msg.get("client_id") == client_id:
                        self.logger.info(f"Reprocessing message for client {client_id}")
                        self.credits_producer.enqueue(msg)
                    else:
                        writing_file.write(line)

            os.replace(temp_path, PENDING_MESSAGES)

        if not self.consumer.is_alive():
            self.consumer.start()
            self.logger.info("Thread de consumo de credits empezado")
        else:
            self.logger.info("Thread de consumo de credits no empezado, ya existe uno")

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
                    if len(lines) != 2 or not lines[0].startswith("BEGIN_TRANSACTION;") or lines[1] != "END_TRANSACTION;":
                        self.logger.error(f"Formato inválido en archivo {filename}. Se omite.")
                        # TODO si es invalido borrar el archivo
                        continue

                    raw_json = lines[0][len("BEGIN_TRANSACTION;"):]
                    current_payload = json.loads(raw_json)

                    self.movies[client_id] = current_payload
                    # self.results[client_id] = {}
                    self.has_recovered_at_least_one = True
                    self.logger.info(f"Películas recuperadas para cliente {client_id}: {len(self.movies[client_id])} items.")

            except json.JSONDecodeError as e:
                self.logger.exception(f"Error decodificando JSON en archivo {filename}: {e}")
            except Exception as e:
                self.logger.exception(f"Error al intentar recuperar películas desde archivo {filename}: {e}")


    def start(self):
        self.logger.info("Iniciando joiner de credits")
        try:
            self.movies_consumer.start()
            self.control_consumer.start()
            self.shutdown_event.wait()
        finally:
            self.close()


if __name__ == '__main__':
    worker = CreditsJoiner()
    worker.start()

import json
import os
import random
import string
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from utils.parsers.credits_parser import convert_data
from worker.abstractaggregator.abstractaggregator import AbstractAggregator

PENDING_MESSAGES = "/root/files/credits_pending.jsonl"


class CreditsJoiner(AbstractAggregator):
    def __init__(self):
        super().__init__()
        # TODO obtener de una envar
        self.joiner_instance_id = "joiner_credits"
        self.processed_rating_batches_per_client = defaultdict(int)
        self.movies_consumer = Subscriber("20_century_arg_result",
                                          message_handler=self.handle_movies_message)
        self.credits_producer = Producer(
            queue_name="credits",
            queue_type="direct")
        self.results = {}
        self.movies = {}
        self.control_consumer = Consumer("joiner_control_credits", _message_handler=self.handle_control_message)

    def create_consumer(self):
        return Consumer("credits", _message_handler=self.handle_message)

    def create_producer(self):
        return Producer("top_10_actors_from_batch")

    def process_message(self, client_id, message):
        actors = convert_data(message)
        movies_per_client = self.movies.get(client_id, set())
        partial_result = {}
        for actor in actors:
            if actor.movie_id in movies_per_client:
                actor_id = actor.id
                actor_name = actor.name
                if actor_id not in partial_result:
                    partial_result[actor_id] = {"name": actor_name, "count": 1}
                else:
                    partial_result[actor_id]["count"] += 1
        return partial_result

    def aggregate_message(self, client_id, result):
        if not self.results.get(client_id):
            self.results[client_id] = result
        else:
            for actor_id, actor_data in result.items():
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
        self.processed_rating_batches_per_client.pop(client_id)

    @staticmethod
    def generate_batch_id(client_id, joiner_id):
        rand_str = ''.join(random.choices(string.ascii_uppercase, k=4))

        return f"credits-{joiner_id}-{rand_str}"

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
        control_message = {
            "type": "control",
            "client_id": client_id,
            "batch_id": batch_id,
            "batch_size": self.received_batches_per_client[client_id],
            "joiner_instance_id": self.joiner_instance_id,
        }
        if client_id in self.total_batches_per_client:
            control_message["total_batches"] = self.total_batches_per_client[client_id]
        self.producer.enqueue(control_message)
        self.logger.info(f"Control enviado al aggregator: {control_message}")

    def get_result(self, client_id):
        top_10 = sorted(self.results[client_id].items(), key=lambda item: item[1]["count"], reverse=True)[:10]
        self.logger.info("Top 10 actores con más contribuciones:")
        return top_10

    def handle_movies_message(self, message):
        self.logger.info(f"Mensaje de movies recibido - cliente: " + str(message.get("client_id")))
        if message.get("type") == "20_century_arg_total_result":
            self.process_movie_message(message)
        else:
            self.logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")

    def process_movie_message(self, message):
        # TODO chequear que sea por cliente y no todo el archivo
        client_id = message.get("client_id")
        self.movies[client_id] = {movie["id"] for movie in message.get("movies")}
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

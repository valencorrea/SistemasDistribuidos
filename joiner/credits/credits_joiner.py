import json
import logging
import os
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from middleware.producer.publisher import Publisher
from utils.parsers.credits_parser import convert_data
from worker.worker import Worker

PENDING_MESSAGES = "/root/files/credits_pending.jsonl"



class CreditsJoiner(Worker):
    def __init__(self):
        super().__init__()
        self.received_credits_batches_per_client = {}
        self.total_credits_batches_per_client = {}

        self.processed_rating_batches_per_client = defaultdict(int)
        self.movies_consumer = Subscriber("20_century_arg_result",
                                        message_handler=self.handle_movies_message)
        self.credits_consumer = Consumer("credits",
                                        _message_handler=self.handle_credits_message)
        self.producer = Producer("top_10_actors_from_batch")
        self.credits_producer = Producer(
            queue_name="credits",
            queue_type="direct")
        self.actor_counts = {}
        self.movies = {}
        self.client_id = None
        self.received_credits_batches_per_client = {}
        self.total_credits_batches_per_client = {}
        self.amounts_control_producer = Publisher("credits_amounts")
        self.amounts_control_consumer = Subscriber("credits_amounts",
                                                   message_handler=self.handle_amounts)

    def handle_amounts(self, message):
        try:
            self.logger.info(f"Mensaje de control de credits recibido")
            message_type = message.get("type")
            client_id = message.get("client_id")
            amount = int(message.get("amount", 0))
            if message_type == "total_batches":
                self.total_credits_batches_per_client[client_id] = amount
                self.logger.info(f"Actualizado total_batches {amount} a {self.total_credits_batches_per_client[client_id] }")
            elif message_type == "batch_size":
                self.received_credits_batches_per_client[client_id] = self.received_credits_batches_per_client.get(client_id, 0) + amount
                self.logger.info(f"Actualizado batch_size {amount} a {self.received_credits_batches_per_client[client_id] }")
            else:
                self.logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message_type}")
                return

            total = self.total_credits_batches_per_client.get(client_id, None)
            received = self.received_credits_batches_per_client.get(client_id, 0)
            self.logger.info(f"Control de credits total: {total} received: {received}")
            if total is not None and 0 < total <= received:
                top_10 = self.get_result(client_id)
                result_message = {
                    "type": "query_4_top_10_actores_credits",
                    "actors": top_10,
                    "client_id": client_id,
                    "processed_batches": self.processed_rating_batches_per_client.get(client_id, 0),
                    "total_batches": self.total_credits_batches_per_client.get(client_id, 0)
                }
                self.producer.enqueue(result_message)
                self.logger.info(f"Resultado enviado {result_message}.")
                self.actor_counts.pop(client_id)
                self.total_credits_batches_per_client.pop(client_id)
                self.received_credits_batches_per_client.pop(client_id)
                self.processed_rating_batches_per_client.pop(client_id)

        except Exception as e:
            self.logger.error(f"Error al procesar mensaje <{message}> de control de cantidades: {e}")

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.movies_consumer.close()
            self.credits_consumer.close()
            self.producer.close()
            self.credits_producer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

    def handle_credits_message(self, message):
        client_id = message.get("client_id")
        try:
            self.logger.info(f"Mensaje de credits recibido - cliente: " + str(message.get("client_id")))

            if client_id not in self.movies.keys():
                os.makedirs(os.path.dirname(PENDING_MESSAGES), exist_ok=True)
                self.logger.info("client id " + client_id + " not ready for credits file. Saving locally")
                with open(PENDING_MESSAGES, "a") as f:
                    f.write(json.dumps(message) + "\n")
                return

            actors = convert_data(message)
            movies_per_client = self.movies.get(client_id, set())
            self.processed_rating_batches_per_client[client_id] += message.get("batch_size", 0)

            for actor in actors:
                if actor.movie_id in movies_per_client:
                    actor_id = actor.id
                    actor_name = actor.name
                    if client_id not in self.actor_counts:
                        self.actor_counts[client_id] = {}
                    if actor_id not in self.actor_counts[client_id]:
                        self.actor_counts[client_id][actor_id] = {"name": actor_name, "count": 1}
                    else:
                        self.actor_counts[client_id][actor_id]["count"] += 1
            total_batches = message.get("total_batches")
            batch_size = message.get("batch_size")
            if total_batches != 0: # Mensaje que contiene el total. Uno por cliente.
                self.logger.info(f"Se envia la cantidad total de batches: {total_batches}.")
                self.amounts_control_producer.enqueue({"type": "total_batches","amount": total_batches, "client_id": client_id})

            if batch_size!= 0:
                self.logger.info(f"Se envia la cantidad de este batch: {batch_size}.")
                self.amounts_control_producer.enqueue({"type": "batch_size", "amount": batch_size, "client_id": client_id})

        except Exception as e:
            self.logger.exception(f"❌ Error al procesar credits para client_id={client_id}: {e}")
            self.close()

    def get_result(self, client_id):
        top_10 = sorted(self.actor_counts[client_id].items(), key=lambda item: item[1]["count"], reverse=True)[:10]
        self.logger.info("Top 10 actores con más contribuciones:")
        return top_10

    def handle_movies_message(self, message):
        self.logger.info(f"Mensaje de movies recibido - cliente: " + str(message.get("client_id")))
        if message.get("type") == "20_century_arg_total_result":
            self.process_movie_message(message)
        else:
            self.logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")

    def process_movie_message(self, message):
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

        if not self.credits_consumer.is_alive():
            self.credits_consumer.start()
            self.logger.info("Thread de consumo de credits empezado")
        else:
            self.logger.info("Thread de consumo de credits no empezado, ya existe uno")

    def start(self):
        self.logger.info("Iniciando joiner de credits")
        try:
            self.amounts_control_consumer.start()
            self.movies_consumer.start()
            self.shutdown_event.wait()
        finally:
            self.close()

if __name__ == '__main__':
    worker = CreditsJoiner()
    worker.start()

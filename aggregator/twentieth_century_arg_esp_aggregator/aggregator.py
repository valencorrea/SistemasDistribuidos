import json
import logging
import time

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker
import os

class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("aggregate_consulta_1",
                                 _message_handler=self.handle_message)
        self.producer = Producer("result")
        self.results = {} # {cliente:movies}
        self.control_batches_per_client = {}
        self.total_batches_per_client = {}

        self.log_file = "resultados.log"
        self.processed_batch_ids = set()
        self.load_processed_batches(self.log_file)


    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):
        if message.get("type") != "batch_result":
            return

        client_id = message.get("client_id")
        batch_id = str(message.get("batch_id"))
        batch_size = message.get("batch_size", 0)
        total_batches = message.get("total_batches")

        if not client_id:
            self.logger.error(f"Mensaje inválido: falta client_id en batch {batch_id}")
            self.consumer.ack(batch_id)
            return

        if batch_id in self.processed_batch_ids:
            self.logger.info(f"Batch {batch_id} ya procesado. Enviando ACK sin reprocesar.")
            self.consumer.ack(batch_id)
            return

        if client_id not in self.results:
            self.results[client_id] = []
            self.control_batches_per_client[client_id] = 0

        self.results[client_id].extend(message.get("movies", []))
        self.control_batches_per_client[client_id] += batch_size

        if total_batches:
            self.total_batches_per_client[client_id] = total_batches

        self.logger.info(f"Batch procesado. Películas acumuladas: {len(self.results[client_id])} cliente {client_id}")

        self.persist_result(batch_id, batch_size, client_id, message, total_batches)

        self.processed_batch_ids.add(batch_id)

        if self.total_batches_per_client.get(client_id) and \
                self.total_batches_per_client[client_id] <= self.control_batches_per_client[client_id]:
            result_message = {
                "result_number": 1,
                "type": "query_1_arg_esp_2000",
                "result": self.results[client_id],
                "total_movies": len(self.results[client_id]),
                "client_id": client_id,
                "batch_id": batch_id
            }
            if self.producer.enqueue(result_message):
                self.logger.info(f"Resultado final enviado con {len(self.results[client_id])} películas")
                self.results.pop(client_id)
                self.control_batches_per_client.pop(client_id)
                self.total_batches_per_client.pop(client_id)

        self.consumer.ack(batch_id)

    def persist_result(self, batch_id, batch_size, client_id, message, total_batches):
        with open(self.log_file, "a") as f:
            payload = {
                "client_id": client_id,
                "movies": message.get("movies", []),
                "batch_size": batch_size,
                "total_batches": total_batches
            }
            f.write(f"BEGIN_TRANSACTION;{batch_id};{json.dumps(payload)}\n")
            f.write(f"END_TRANSACTION;{batch_id}\n")

    def start(self):
        self.logger.info("Iniciando agregador")
        self.consumer.start_consuming_2()

    def load_processed_batches(self, log_file="resultados.log"):
        self.logger.debug("Iniciando proceso de recuperación.")
        self.results = {}
        self.control_batches_per_client = {}
        self.total_batches_per_client = {}

        if not os.path.exists(log_file):
            return

        self.logger.info("Se encontró un archivo de recuperación. Recreando estado.")
        in_transaction = False
        current_batch_id = None
        current_payload = None

        with open(log_file, "r") as f:
            try:
                for line in f:
                    parts = line.strip().split(";", 2)

                    if parts[0] == "BEGIN_TRANSACTION" and len(parts) == 3:
                        in_transaction = True
                        current_batch_id = parts[1]
                        current_payload = json.loads(parts[2])
                        self.logger.info(f"Se encontro una transaccion para el id {current_batch_id}.")

                    elif parts[0] == "END_TRANSACTION" and len(parts) == 2 and in_transaction:
                        batch_id = parts[1]
                        if batch_id != current_batch_id:
                            self.logger.error(f"Mismatch de batch_id en transacción: {batch_id} != {current_batch_id}")
                            continue

                        client_id = current_payload.get("client_id")
                        movies = current_payload.get("movies", [])
                        batch_size = current_payload.get("batch_size", 0)
                        total_batches = current_payload.get("total_batches")

                        if not client_id:
                            self.logger.error(f"Error: No se encontró client_id en el batch {batch_id}")
                            continue

                        if client_id not in self.results:
                            self.results[client_id] = []
                            self.control_batches_per_client[client_id] = 0
                            self.logger.info(f"Se encontro otro cliente en el archivo de recuperacion {client_id}.")

                        self.results[client_id].extend(movies)
                        self.control_batches_per_client[client_id] += batch_size

                        if total_batches is not None:
                            self.total_batches_per_client[client_id] = total_batches

                        self.processed_batch_ids.add(batch_id)
                        self.logger.info(f"Se recupero la transaccion con id {current_batch_id}.")

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
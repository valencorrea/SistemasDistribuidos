import json
import os
import time
from abc import abstractmethod
from collections import defaultdict

from worker.worker import Worker


class AbstractAggregator(Worker):
    def __init__(self):
        super().__init__()
        self.total_batches_per_client = defaultdict(int)
        self.received_batches_per_client = defaultdict(int)
        self.processed_batch_ids = set()
        self.logname = "_resultados.log"
        self.results = {}
        self.producer = self.create_producer()
        # Es importante que se procese antes de comenzar a leer de nuevo
        # TODO revisar el caso borde del ultimo batch si es que se vuelve de una recuperacion
        self.load_processed_batches()
        self.consumer = self.create_consumer()

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            # TODO borrar el log file
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

    def start(self):
        self.logger.info("Iniciando agregador")
        self.consumer.start_consuming_2()

    def handle_message(self, message):
        batch_size = message.get("batch_size", None)
        total_batches = message.get("total_batches")
        client_id = message.get("client_id", None)
        batch_id = message.get("batch_id", None)

        batch_size = int(batch_size) if batch_size is not None else None
        total_batches = int(total_batches) if total_batches is not None else None
        client_id = str(client_id) if client_id is not None else None
        batch_id = str(batch_id) if batch_id is not None else None

        if not batch_id:
            self.logger.error(f"Mensaje malformado: falta batch_id")

        if message.get("type") != "batch_result":
            self.consumer.ack(batch_id)
            return

        if not client_id or not batch_size:
            self.logger.error(f"Mensaje malformado: falta client_id o batch_size en batch {batch_id}")
            self.consumer.ack(batch_id)
            return

        if batch_id in self.processed_batch_ids:
            self.logger.info(f"Batch {batch_id} ya procesado. Enviando ACK sin reprocesar.")
            self.consumer.ack(batch_id)
            return

        if client_id not in self.results:
            self.logger.info(f"Se recibio un nuevo cliente con id {client_id}.")
            # self.results[client_id] = []
            self.received_batches_per_client[client_id] = 0

        self.received_batches_per_client[client_id] += batch_size
        self.logger.info(f"Se actualiza la cantidad recibida del cliente {client_id}: {batch_size}, actual: {self.received_batches_per_client[client_id]}.")

        if total_batches:
            self.total_batches_per_client[client_id] = total_batches
            self.logger.info(f"Se actualiza la cantidad total de batches para el cliente {client_id}: {self.total_batches_per_client[client_id]}.")

        # En estos tres pasos se procesa, persiste y agrega el mensaje
        result = self.process_message(client_id, message)
        self.persist_result(client_id, batch_id, batch_size, total_batches, result)
        self.consumer.ack(batch_id)
        self.aggregate_message(client_id, result)
        self.processed_batch_ids.add(batch_id)

        self.logger.info(f"Fue procesado el mensaje {batch_id} del cliente {client_id}")
        self.check_if_its_completed(client_id)

    def check_if_its_completed(self, client_id):
        if self.total_batches_per_client[client_id] and self.received_batches_per_client[client_id] >= self.total_batches_per_client[client_id]:
            self.logger.info(f"Se va a enviar todo para el cliente {client_id}.")
            self.send_aggregated_result(client_id)
            self.logger.info(f"Se envio el resultado para el cliente {client_id}.")
            self.delete_client(client_id)

    def delete_client(self, client_id):
        try:
            self.logger.info(f" Se va a borrar el cliente {client_id} de  los clientes {self.results.keys()}")
            self.results.pop(client_id)
            self.total_batches_per_client.pop(client_id)
            self.received_batches_per_client.pop(client_id)
            # Si se cae aca, no se borra el log del cliente y el resultado se envia dos veces. Suponemos que no es un problema
            self.delete_log_for_client(client_id)
        except KeyError:
            self.logger.exception(f"Error al eliminar el resultado del cliente {client_id}")

    def delete_log_for_client(self, client_id):
        log_file = f"{client_id}{self.logname}"
        if os.path.exists(log_file):
            os.remove(log_file)
            self.logger.info(f"Archivo de log {log_file} eliminado.")
        else:
            self.logger.warning(f"Archivo de log {log_file} no encontrado, no se puede eliminar.")

    def persist_result(self, client_id, batch_id, batch_size, total_batches, result):
        # TODO Cada tanto crear un acumulado de log y borrar lo viejo
        log_file = f"{client_id}{self.logname}"
        payload = {
            "client_id": client_id,
            "result": result,
            "batch_size": batch_size
        }
        if total_batches is not None:
            payload["total_batches"] = total_batches

        with open(log_file, "a") as f:
            f.write(f"BEGIN_TRANSACTION;{batch_id};{json.dumps(payload)}\n")
            f.write(f"END_TRANSACTION;{batch_id}\n")

    def load_processed_batches(self):
        self.logger.debug("Iniciando proceso de recuperación.")
        for filename in os.listdir():
            if not filename.endswith(self.logname):
                self.logger.info(f"Archivo {filename} no es un archivo de resultados, se omite.")
                continue

            client_id = filename.replace("%s" % self.logname, "")
            self.logger.info(f"Recuperando estado para cliente {client_id} desde {filename}")

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
                                self.logger.error(f"Mismatch de batch_id en transacción: {batch_id} != {current_batch_id}")
                                continue

                            client_id = current_payload.get("client_id")
                            result = current_payload.get("result", [])
                            batch_size = current_payload.get("batch_size", 0)
                            total_batches = current_payload.get("total_batches")

                            if client_id not in self.results:
                                self.results[client_id] = []
                                self.received_batches_per_client[client_id] = 0
                                self.logger.info(f"Nuevo cliente recuperado: {client_id}")

                            self.aggregate_message(client_id, result)
                            self.received_batches_per_client[client_id] += batch_size

                            if total_batches is not None:
                                self.total_batches_per_client[client_id] = total_batches

                            self.processed_batch_ids.add(batch_id)
                            self.logger.info(f"Transacción recuperada con id {current_batch_id}.")

                            # Reset
                            in_transaction = False
                            current_batch_id = None
                            current_payload = None

                except json.JSONDecodeError as e:
                    self.logger.exception(f"Error decodificando JSON de batch {current_batch_id}: {e}")
                except Exception as e:
                    self.logger.exception(f"Error al intentar recuperar el archivo de log {current_batch_id}: {e}")
                    exit(1)
        self.recheck_if_some_client_is_completed_after_restart()

    def recheck_if_some_client_is_completed_after_restart(self):
        for client_id in list(self.results.keys()):
            self.check_if_its_completed(client_id)

    def send_aggregated_result(self, client_id):
        result_message = self.create_final_result(client_id)
        self.logger.info(f"Enviando resultado final para el cliente {client_id} por la cola {self.producer.getname()}")
        if self.producer.enqueue(result_message):
            self.logger.info(f"Resultado final enviado con {len(self.results[client_id])} películas al cliente {client_id}")
        else:
            self.logger.error(f"Error al enviar el resultado final en el cliente {client_id}")
            # TODO No estamos considerando los casos con error de conexion, deberiamos?

    @abstractmethod
    def process_message(self, client_id, message):
        pass

    @abstractmethod
    def aggregate_message(self, client_id, result):
        pass

    @abstractmethod
    def create_final_result(self, client_id):
        pass

    @abstractmethod
    def create_consumer(self):
        pass

    @abstractmethod
    def create_producer(self):
        pass
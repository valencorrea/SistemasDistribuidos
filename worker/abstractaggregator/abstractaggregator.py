import json
import os
import shutil
import time
from abc import abstractmethod
from collections import defaultdict

from middleware.consumer.subscriber import Subscriber
from worker.worker import Worker


class AbstractAggregator(Worker):
    def __init__(self):
        super().__init__()
        self.max_file_size = 100 * 1024  # 1kB masomenos
        self.total_batches_per_client = defaultdict(int)
        self.received_batches_per_client = defaultdict(int)
        self.processed_batch_ids = set()
        self.results_log_name = "_resultados.log"
        self.results = {}
        self.producer = self.create_producer()
        # Es importante que se procese antes de comenzar a leer de nuevo
        # TODO revisar el caso borde del ultimo batch si es que se vuelve de una recuperacion
        self.load_processed_batches()
        self.consumer = self.create_consumer()
        self.clients_announcer_subscriber = Subscriber("clients_announcer", message_handler=self.clean_all_clients)
        self.clients_announcer_subscriber.start()

    def clean_all_clients(self, message):
        try:
            active_clients = message["active_clients"]
            self.logger.info("Recibido mensaje de limpieza de clientes: Activos: %s", active_clients)
            for client_id in list(self.results.keys()) not in active_clients:
                self.delete_client(client_id)
        except:
            self.logger.exception("Error al procesar el mensaje de limpieza de clientes. Se ignora el mensaje.")

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
        total_batches = message.get("total_batches", None)
        client_id = message.get("client_id", None)
        batch_id = message.get("batch_id", None)

        batch_size = int(batch_size) if batch_size is not None else None
        if total_batches is not None:
            self.logger.info(f"Mensaje de total batches recibido: {total_batches} para cliente {client_id}.")
            total_batches = int(total_batches)
        client_id = str(client_id) if client_id is not None else None
        batch_id = str(batch_id) if batch_id is not None else None

        if not batch_id:
            self.consumer.ack(batch_id)
            self.logger.error(f"Mensaje malformado: falta batch_id")

        # if message.get("type") != "batch_result":
        #
        #     self.consumer.ack(batch_id)
        #     return

        if not client_id or batch_size is None:
            self.logger.error(f"Mensaje malformado: falta client_id o batch_size en batch {batch_id}")
            self.consumer.ack(batch_id)
            return

        if batch_id in self.processed_batch_ids:
            self.logger.info(f"Batch {batch_id} ya procesado. Enviando ACK sin reprocesar.")
            self.consumer.ack(batch_id)
            return

        if client_id not in self.results:
            self.logger.info(f"Se recibio un nuevo cliente con id {client_id}.")
            self.received_batches_per_client[client_id] = 0

        self.received_batches_per_client[client_id] += batch_size
        self.logger.info(
            f"Se actualiza la cantidad recibida del cliente {client_id}: {batch_size}, actual: {self.received_batches_per_client[client_id]}.")

        if total_batches is not None:
            self.total_batches_per_client[client_id] = total_batches
            self.logger.info(
                f"Se actualiza la cantidad total de batches para el cliente {client_id}: {self.total_batches_per_client[client_id]}.")

        # En estos tres pasos se procesa, persiste y agrega el mensaje
        result = self.process_message(client_id, message)
        if result is None: # Esto es para el caso del que en el aggregator no tengo el movies todavia
            self.logger.info(f"Este mensaje se persistio {batch_id} del cliente {client_id}, se va a prcesar luego.")
            self.consumer.ack(batch_id)
            return
        current_file_size = self.persist_result(client_id, batch_id, batch_size, total_batches, result)
        self.consumer.ack(batch_id)
        self.send_batch_processed(client_id, batch_id, batch_size, total_batches)
        self.aggregate_message(client_id, result)
        self.processed_batch_ids.add(batch_id)

        self.logger.info(f"Fue procesado el mensaje {batch_id} del cliente {client_id}")
        self.logger.info(f"Tamaño actual del archivo de log: {current_file_size} bytes")
        if self.check_if_its_completed(client_id):
            return
        elif current_file_size > self.max_file_size:
            self.logger.info(
                f"El archivo de log del cliente {client_id} ha superado el tamaño máximo de {self.max_file_size} bytes.")
            # Aca capaz agregar una condicion para no revisar siempre el tamaño del archivo
            self.compact_log_for_client(client_id)

    def send_batch_processed(self, client_id, batch_id, batch_size, total_batches):
        # TODO abstraer en otra clase para que no meta ruido aca. Esto es para los joiners
        pass

    def check_if_its_completed(self, client_id):
        if client_id not in self.total_batches_per_client:
            return False
        if self.received_batches_per_client[client_id] >= self.total_batches_per_client[client_id]:
            self.logger.info(f"Se va a enviar todo para el cliente {client_id}.")
            self.send_aggregated_result(client_id)
            self.logger.info(f"Se envio el resultado para el cliente {client_id}")
            self.delete_client(client_id)
            return True
        return False

    def delete_client(self, client_id):
        try:
            self.logger.info(f" Se va a borrar el cliente {client_id} de  los clientes {self.results.keys()}")
            self.results.pop(client_id)
            self.total_batches_per_client.pop(client_id)
            self.received_batches_per_client.pop(client_id)
            # Si se cae aca, no se borra el log del cliente y el resultado se envia dos veces. Suponemos que no es un problema
            self.delete_file(f"{client_id}{self.results_log_name}")
        except KeyError:
            self.logger.exception(f"Error al eliminar el resultado del cliente {client_id}")

    def delete_file(self, log_file):
        if os.path.exists(log_file):
            os.remove(log_file)
            self.logger.info(f"Archivo de log {log_file} eliminado.")
        else:
            self.logger.warning(f"Archivo de log {log_file} no encontrado para eliminar.")

    def persist_result(self, client_id, batch_id, batch_size, total_batches, result) -> int:
        # TODO Cada tanto crear un acumulado de log y borrar lo viejo
        log_file = f"{client_id}{self.results_log_name}"
        payload = {
            "client_id": client_id,
            "result": result,
            "batch_size": batch_size
        }
        if total_batches is not None:
            payload["total_batches"] = total_batches

        return self.persist_entry(batch_id, log_file, payload)

    @staticmethod
    def persist_entry(batch_id, log_file, payload) -> int:
        with open(log_file, "a") as f:
            f.write(f"BEGIN_TRANSACTION;{batch_id};{json.dumps(payload)}\n")
            f.write(f"END_TRANSACTION;{batch_id}\n")
            # Hago el chequeo aca porque ya tengo el file descriptor abierto
            return os.fstat(f.fileno()).st_size

    def load_processed_batches(self):
        self.logger.debug("Iniciando proceso de recuperación.")
        self.check_for_incomplete_backups()
        for filename in os.listdir():
            if not filename.endswith(self.results_log_name):
                self.logger.info(f"Archivo {filename} no es un archivo de resultados, se omite.")
                continue

            client_id = filename.replace("%s" % self.results_log_name, "")
            self.logger.info(f"Recuperando estado para cliente {client_id} desde {filename}")

            in_transaction = False
            current_batch_id = None
            current_payload = None

            with open(filename, "r") as f:
                try:
                    for line in f:
                        parts = line.strip().split(";", 2)

                        if parts[0] == "ID" and len(parts) == 2:
                            batch_id = parts[1]
                            self.processed_batch_ids.add(batch_id)
                            self.logger.info(f"Marcado batch_id {batch_id} como procesado desde entrada compactada.")
                            continue

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

                            client_id = current_payload.get("client_id")
                            result = current_payload.get("result", [])
                            batch_size = current_payload.get("batch_size", 0)
                            total_batches = current_payload.get("total_batches")

                            if client_id not in self.results:
                                # self.results[client_id] = {}
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
                            # TODO si el archivo es invalido, deberiamos borrarlo
                except json.JSONDecodeError as e:
                    self.logger.exception(f"Error decodificando JSON de batch {current_batch_id}: {e}")
                except Exception as e:
                    self.logger.exception(f"Error al intentar recuperar el archivo de log {current_batch_id}: {e}")
                    exit(1)
        self.recheck_if_some_client_is_completed_after_restart()

    def check_for_incomplete_backups(self):
        for filename in os.listdir():
            if not filename.endswith(f"{self.results_log_name}_compacted"):
                continue

            base_filename = filename.replace("_compacted", "")
            log_file = base_filename
            compacted_file = filename

            try:
                # Verificar si el archivo compactado termina con END_TRANSACTION para ver si es válido
                with open(compacted_file, "r") as f:
                    lines = f.readlines()
                    if not lines or not any(line.startswith("END_TRANSACTION") for line in reversed(lines[-5:])):
                        self.logger.warning(
                            f"Archivo compactado {compacted_file} parece incompleto. Se mantiene el archivo original.")
                        os.remove(compacted_file)
                        self.logger.info(f"Archivo compactado inválido {compacted_file} eliminado.")
                        continue

                if os.path.exists(log_file):  # Como el compactado esta bien, borro el original
                    os.remove(log_file)
                    self.logger.info(f"Archivo original {log_file} eliminado porque hay un archivo compactado válido.")

                # Esto es bastante costoso, pero sino la operacion de renombre no es atomica. Igual es un caso bastante
                # borde que no deberia pasar: que al hacer la compactacion no se haya renombrado el archivo
                shutil.copy2(compacted_file, log_file)
                self.logger.info(f"Archivo {compacted_file} copiado de forma segura a {log_file}.")
                os.remove(compacted_file)
                self.logger.info(f"Archivo temporal {compacted_file} eliminado tras copia segura.")
                self.fsync_dir()

            except Exception as e:
                self.logger.exception(f"Error al intentar validar o reemplazar archivo compactado: {e}")

    def recheck_if_some_client_is_completed_after_restart(self):
        for client_id in list(self.results.keys()):
            self.check_if_its_completed(client_id)

    def send_aggregated_result(self, client_id):
        result_message = self.create_final_result(client_id)
        self.logger.info(
            f"Enviando resultado final para el cliente {client_id} por la cola {self.producer.getname()}: {result_message}")
        if self.producer.enqueue(result_message):
            self.logger.info(
                f"Resultado final enviado con {len(self.results[client_id])} películas al cliente {client_id}")
        else:
            self.logger.error(f"Error al enviar el resultado final en el cliente {client_id}")
            # TODO No estamos considerando los casos con error de conexion, deberiamos?

    def compact_log_for_client(self, client_id):
        log_file = f"{client_id}{self.results_log_name}"
        if not os.path.exists(log_file):
            self.logger.warning(f"No existe el archivo de log para el cliente {client_id}, no se puede compactar.")
            return

        self.logger.info(f"Iniciando compactación del archivo de log para el cliente {client_id}.")

        aggregated_result = self.results.get(client_id)
        total_batches = self.total_batches_per_client.get(client_id, None)
        received_batches = self.received_batches_per_client.get(client_id, 0)

        # Filtrar solo los batch_ids del cliente actual
        client_batch_ids = [bid for bid in self.processed_batch_ids if bid.startswith(client_id)]

        if aggregated_result is None or received_batches == 0 or not client_batch_ids:
            self.logger.warning(f"No hay datos suficientes para compactar el log del cliente {client_id}.")
            return

        try:
            with open(log_file + "_compacted", "w") as f:
                # Se usa un formato mas liviano para guardar las entradas que solo tienen id
                for batch_id in client_batch_ids[:-1]:
                    f.write(f"ID;{batch_id}\n")

                compacted_batch_id = client_batch_ids[-1]  # Se usa el ultimo batch_id para el compactado
                payload = {
                    "client_id": client_id,
                    "result": aggregated_result,
                    "batch_size": received_batches
                }
                if total_batches:
                    payload["total_batches"] = total_batches
                f.write(f"BEGIN_TRANSACTION;{compacted_batch_id};{json.dumps(payload)}\n")
                f.write(f"END_TRANSACTION;{compacted_batch_id}\n")
                os.fsync(
                    f.fileno())  # Flush al disco, no vaya a ser que todavia lo tengamos en ram. Hace falta si es que ya cerramos el file descriptor?

            self.logger.info(f"Archivo de log compactado exitosamente para el cliente {client_id}.")
            os.remove(log_file)
            os.rename(log_file + "_compacted", log_file)
            self.fsync_dir()

        except Exception:
            self.logger.exception(
                f"Error durante la compactación del log para el cliente {client_id}.")

    @staticmethod
    def fsync_dir():
        # Forzamos la sincronización del directorio para asegurarnos de que los cambios se escriban en disco
        dir_fd = os.open(".", os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

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

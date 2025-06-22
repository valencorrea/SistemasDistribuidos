import json
import logging
import os
from collections import Counter, defaultdict

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.abstractaggregator.abstractaggregator import AbstractAggregator
from worker.worker import Worker

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%H:%M:%S')

class Aggregator(AbstractAggregator):
    def __init__(self):
        super().__init__()
        self.control_received_batches_per_client = defaultdict(int)
        self.batches_by_joiner = defaultdict(set)
        self.tcp_host = os.getenv("AGGREGATOR_HOST", "aggregator_top_10")
        self.tcp_port = int(os.getenv("AGGREGATOR_PORT", 60002))
        #self.server = TCPServer(self.tcp_host, self.tcp_port, self._handle_tcp_message)
        self.joiner_control_publisher = Producer("joiner_control_credits")

    def create_consumer(self):
        return Consumer("top_10_actors_from_batch", _message_handler=self.handle_message_joiner_aggregator)

    def create_producer(self):
        return Producer("result")

    def handle_message_joiner_aggregator(self, message):
        self.logger.info(f"Mensaje recibido en desde top_10_actors_from_batch")
        type_of_message = message.get("type")
        self.logger.info(f"Tipo de mensaje recibido: {type_of_message}")
        if type_of_message == "query_4_top_10_actores_credits":
            self.handle_message(message)
        elif type_of_message == "control":
            self.handle_control_message(message)
        else:
            self.logger.error(f"Tipo de mensaje desconocido: {type_of_message}. Mensaje: {message}")

    def process_message(self, client_id, message):
        actors = message.get("actors")
        self.logger.info(f"Se obtuvieron {len(actors)}: {actors} actores.")
        return actors

    def aggregate_message(self, client_id, actors):
        for _, count in actors:
            self.logger.info(f"Se va a aumentar la cantidad de registros de un actor: {count}: {type(count)}.")
            if client_id not in self.results:
                self.results[client_id] = Counter()
            self.results[client_id][count["name"]] += count["count"]

    def create_final_result(self, client_id):
        final_top_10 = self.results[client_id].most_common(10)
        return {
            "result_number": 4,
            "type": "top_10_actors",
            "actors": final_top_10,
            "client_id": client_id
        }

    def handle_result_message(self, message):
        self.logger.info(f"Mensaje de top 10 parcial recibido {message}")
        client_id = message.get("client_id")
        batch_id = message.get("batch_id")
        batch_size = message.get("batch_size")
        actors = message.get("actors")
        self.logger.info(f"Se obtuvieron {len(actors)}: {actors} actores.")

        if batch_size:
            self.received_batches_per_client[client_id] = self.received_batches_per_client.get(client_id, 0) + batch_size
            self.logger.info(f"Se actualiza la cantidad total de batches: {self.received_batches_per_client[client_id]} para el cliente {client_id}.")

        for _, count in actors:
            self.logger.info(f"Se va a aumentar la cantidad de registros de un actor: {count}: {type(count)}.")
            self.results[client_id][count["name"]] += count["count"]

        total = self.total_batches_per_client.get(client_id, None)
        received = self.received_batches_per_client.get(client_id, 0)
        self.logger.info(f"Control de batches para el cliente {client_id}: total: {total}, received: {received}")
        if total and 0 < total <= received:
            self.logger.info(f"Se va a enviar el resultado final para el cliente {client_id}.")
            # Top 10 final encontrado
            final_top_10 = self.results[client_id].most_common(10)
            self.producer.enqueue({
                "result_number": 4,
                "type": "top_10_actors",
                "actors": final_top_10,
                "client_id": client_id,
                "batch_id": batch_id
            })
            self.logger.info("Top 10 actores agregados y enviados.")
            self.results.pop(client_id)
            self.control_received_batches_per_client.pop(client_id)
            self.total_batches_per_client.pop(client_id)
            self.received_batches_per_client.pop(client_id)


    def _handle_tcp_message(self, msg, addr):
        try:
            self.logger.info(f"[TCP] Mensaje recibido de {addr}: {msg}")
            data_json = json.loads(msg)
            if data_json.get("type") != "batch_id":
                return
            batch_id = data_json.get("batch_id")
            joiner_instance_id = data_json.get("joiner_instance_id")
            if batch_id is None or joiner_instance_id is None:
                self.logger.warning(f"[TCP] batch_id o joiner_instance_id faltante en mensaje: {msg}")
                return
            self.batches_by_joiner[joiner_instance_id].add(str(batch_id))
        except Exception as e:
            self.logger.error(f"[TCP] Error procesando mensaje recibido: {e}")

    def close(self):
        # TODO revisar close de estos aggregators
        self.logger.info("Cerrando conexiones del worker...")
        try:
            #self.server.stop()
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

    def handle_control_message(self, message):
        total_batches = message.get("total_batches")
        batch_size = message.get("batch_size")
        joiner_id = message.get("joiner_instance_id")
        batch_id = message.get("batch_id")
        client_id = message.get("client_id")
        self.control_received_batches_per_client[client_id] = self.control_received_batches_per_client.get(client_id, 0) + batch_size

        self.logger.info(f"Se recibio un mensaje de control para el cliente {client_id} con batch_id {batch_id}.")
        if total_batches:
            self.total_batches_per_client[client_id] = total_batches
            self.logger.info(f"Se actualiza la cantidad total de batches: {self.total_batches_per_client[client_id]} para el cliente {client_id}.")
        self.consumer.ack(batch_id)
        # self.batches_by_joiner[joiner_id].add(batch_id)

        total = self.total_batches_per_client.get(client_id, None)
        received = self.control_received_batches_per_client.get(client_id, 0)
        if total and 0 < total <= received:
            self.logger.info(f"Ya fueron procesados todos los batches ({received}/{total}) para el cliente {client_id}. Enviando el acumulado.")
            self.joiner_control_publisher.enqueue({
                "client_id": client_id
            })
            # TODO una vez que se envio el mensaje de control, borrar los datos del cliente

    def start(self):
        self.logger.info("Iniciando agregador")
        # self.server.start()
        super().start()


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()
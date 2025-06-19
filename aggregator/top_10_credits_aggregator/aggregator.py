import logging
import socket
import threading
import os
import json

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker



from collections import Counter, defaultdict

class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("top_10_actors_from_batch", _message_handler=self.handle_message)
        self.producer = Producer("result")
        self.total_batches_per_client = defaultdict(int)
        self.received_batches_per_client = defaultdict(int)
        self.actor_counter_per_client = defaultdict(Counter)
        self.tcp_host = os.environ.get("AGGREGATOR_TCP_HOST", "0.0.0.0")
        self.tcp_port = int(os.environ.get("AGGREGATOR_TCP_PORT", 60000))
        self.batches_by_joiner = defaultdict(set)
        self.tcp_server_thread = threading.Thread(target=self.tcp_server, daemon=True)
        self.tcp_server_thread.start()

    def tcp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.tcp_host, self.tcp_port))
            server_socket.listen(5)
            self.logger.info(f"[TCP] Servidor escuchando en {self.tcp_host}:{self.tcp_port}")
            while True:
                client_socket, addr = server_socket.accept()
                self.logger.info(f"[TCP] Conexión aceptada de {addr}")
                threading.Thread(target=self.handle_tcp_client, args=(client_socket, addr), daemon=True).start()

    def handle_tcp_client(self, client_socket, addr):
        with client_socket:
            buffer = b""
            while True:
                data = client_socket.recv(4096)
                if not data:
                    break
                buffer += data
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    try:
                        msg = line.decode('utf-8').strip()
                        if not msg:
                            continue
                        self.logger.info(f"[TCP] Mensaje recibido de {addr}: {msg}")
                        data_json = json.loads(msg)
                        if data_json.get("type") != "batch_id":
                            continue
                        batch_id = data_json.get("batch_id")
                        joiner_instance_id = data_json.get("joiner_instance_id")
                        if batch_id is None or joiner_instance_id is None:
                            self.logger.warning(f"[TCP] batch_id o joiner_instance_id faltante en mensaje: {msg}")
                            continue
                        self.batches_by_joiner[joiner_instance_id].add(str(batch_id))
                    except Exception as e:
                        self.logger.error(f"[TCP] Error procesando mensaje recibido: {e}")

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):
        self.logger.info(f"Mensaje de top 10 parcial recibido {message}")
        client_id = message.get("client_id")
        batch_id = message.get("batch_id")
        actors = message.get("actors")
        self.logger.info(f"Se obtuvieron {len(actors)}: {actors} actores.")

        if message.get("processed_batches") is not None and message.get("batch_size") != 0:
            self.received_batches_per_client[client_id] = self.received_batches_per_client[client_id] + int(message.get("processed_batches"))
            self.logger.info(f"Se actualiza la cantidad recibida: {self.received_batches_per_client[client_id]}, actual: {self.received_batches_per_client[client_id]}.")

        if message.get("total_batches") is not None and message.get("total_batches") != 0:
            self.total_batches_per_client[client_id] = int(message.get("total_batches"))
            self.logger.info(f"Se envia la cantidad total de batches: {self.total_batches_per_client[client_id]}.")

        for _, count in actors:
            self.logger.info(f"Se va a aumentar la cantidad de registros de un actor: {count}: {type(count)}.")
            self.actor_counter_per_client[client_id][count["name"]] += count["count"]

        if self.total_batches_per_client[client_id] is not None and self.total_batches_per_client[client_id] != 0 and self.received_batches_per_client[client_id] >= self.total_batches_per_client[client_id]:
            # Top 10 final encontrado
            final_top_10 = self.actor_counter_per_client[client_id].most_common(10)
            self.producer.enqueue({
                "result_number": 4,
                "type": "top_10_actors",
                "actors": final_top_10,
                "client_id": client_id,
                "batch_id": batch_id
            })
            self.logger.info("Top 10 actores agregados y enviados.")
            self.actor_counter_per_client.pop(client_id)
            self.received_batches_per_client.pop(client_id)
            self.total_batches_per_client.pop(client_id)


    def start(self):
        self.logger.info("Iniciando agregador")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()
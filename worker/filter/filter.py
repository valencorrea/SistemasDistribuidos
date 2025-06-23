import json
import os
from abc import abstractmethod
from collections import defaultdict
from time import sleep

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data
from worker.worker import Worker


class Filter(Worker):
    def __init__(self, name, result_name="result"):
        super().__init__()
        self.name = name
        self.result_name = result_name
        self.consumer = self.create_consumer()
        self.producers = self.create_producers()

    def start(self):
        self.logger.info(f"Iniciando filtro {self.name}")
        self.consumer.start_consuming()

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.shutdown_consumer.close()
            for producer in self.producers:
                producer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):

        batch_id = message.get("batch_id")
        if not batch_id:
            self.logger.error(f"Mensaje malformado: falta batch_id")

        client_id = message.get("client_id")
        batch_size = message.get("batch_size")
        total_batches = message.get("total_batches", None)

        if not client_id or not batch_size:
            self.logger.error(f"Mensaje malformado: falta client_id o batch_size en batch {batch_id}")
            return

        if total_batches:
            self.logger.info(f"Se recibio el mensaje {batch_id} con total_batches: {total_batches} del cliente {client_id}")
        else:
            self.logger.info(f"Se recibio {batch_id} del cliente {client_id}")
        result = self.filter(message)

        result = {
            self.result_name: result,
            "batch_size": message.get("batch_size", 0),
            "total_batches": total_batches,
            "type": "batch_result",
            "client_id": client_id,
            "batch_id": batch_id
        }

        for producer in self.producers:
            sleep(1)
            producer.enqueue(result)
            self.logger.info(f"Se envio el resutlado de {batch_id} del cliente {client_id} al productor {producer.getname()}")

    @abstractmethod
    def filter(self, message):
        pass

    @abstractmethod
    def create_consumer(self):
        pass

    @abstractmethod
    def create_producers(self):
        pass
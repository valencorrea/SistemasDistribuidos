from abc import abstractmethod

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
        if message.get("is_final", False):
            client_id = message.get("client_id")
            if client_id:
                self.logger.info(f"Recibido mensaje envenenado para cliente {client_id}, limpiando datos...")
                self.clean_client(client_id)
                
                # Reenviar mensaje envenenado al aggregator para que también limpie
                poisoned_control_message = {
                    "type": "control",
                    "client_id": client_id,
                    "batch_id": message.get("batch_id"),
                    "batch_size": 0,
                    "joiner_id": self.joiner_instance_id,
                    "is_final": True
                }
                if self.producer:
                    self.producer.enqueue(poisoned_control_message)
                    self.logger.info(f"Mensaje envenenado reenviado al aggregator para cliente {client_id}")
                
                batch_id = message.get("batch_id")
                if batch_id and self.consumer:
                    self.consumer.ack(batch_id)
                self.logger.info(f"Datos del cliente {client_id} limpiados, mensaje envenenado confirmado")
            return

        batch_id = message.get("batch_id")
        if not batch_id:
            self.logger.error(f"Mensaje malformado: falta batch_id")

        client_id = message.get("client_id")
        batch_size = message.get("batch_size", None)
        total_batches = message.get("total_batches", None)

        if not client_id or batch_size is None:
            self.logger.error(f"Mensaje malformado: falta client_id o batch_size en batch {batch_id}: {message}")
            return

        if total_batches is not None:
            self.logger.info(f"Se recibio el mensaje {batch_id} con total_batches: {total_batches} del cliente {client_id}")
        result = self.filter(message)

        result = {
            self.result_name: result,
            "batch_size": message.get("batch_size", 0),
            "type": "batch_result",
            "client_id": client_id,
            "batch_id": batch_id
        }

        if total_batches is not None:
            result["total_batches"] = total_batches

        for producer in self.producers:
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
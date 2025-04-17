import time

from middleware.consumer.consumer import Consumer

class TwentiethCenturyFilter:
    def __init__(self, queue_name='cola'):
        self.consumer = Consumer(
            queue_name=queue_name,
            message_factory=self.handle_message
        )

    def handle_message(self, message_bytes: bytes):
        import json
        message = json.loads(message_bytes.decode())
        print(f"[CONSUMER_CLIENT] Mensaje recibido: {message}")
        return message

    def start(self):
        try:
            print("[CONSUMER_CLIENT] Iniciando consumo de mensajes...")
            self.consumer.start()
        except KeyboardInterrupt:
            print("[CONSUMER_CLIENT] Interrumpido por el usuario")
            #self.close()

    #def close(self):
        #self.consumer.close()


if __name__ == '__main__':
    time.sleep(5)  # Espera para asegurarse que RabbitMQ está listo
    client = TwentiethCenturyFilter()
    client.start()
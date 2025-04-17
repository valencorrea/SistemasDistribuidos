import threading
import time

from consumer.consumer import Consumer
from producer.producer import Producer

class Client:
    def __init__(self):
        self.messages = []
        self.producer = Producer("cola")

        self.consumer = Consumer(queue_name="cola", message_factory=self.handle_message)


    def handle_message(self, message_bytes: bytes):
        import json
        message = json.loads(message_bytes.decode())
        self.messages.append(message)
        print(f"[CLIENT] Mensaje recibido por el consumer: {message}")
        return message

    def start_consumer(self):
        def run_consumer():
            self.consumer.start()

        thread = threading.Thread(target=run_consumer, daemon=True)
        thread.start()
        time.sleep(1)  # Espera a que el consumidor se conecte
        print("[CLIENT] Consumidor iniciado")

    def send_and_receive(self, message: dict):
        print(f"[CLIENT] Enviando mensaje: {message}")
        self.producer.enqueue(message)
        time.sleep(2)  # Espera un poco a que el mensaje se reciba
        if self.messages:
            return self.messages[-1]
        return None


if __name__ == '__main__':
    time.sleep(10)
    client = Client()



    client.start_consumer()

    result = client.send_and_receive({"cola": "mensaje de prueba"})

    if result:
        print(f"[MAIN] Mensaje recibido correctamente: {result}")
    else:
        print("[MAIN] No se recibió ningún mensaje")

    #client.consumer.close()

import time
import json
from middleware.producer.producer import Producer
from middleware.consumer.consumer import Consumer

class Client:
    def __init__(self):
        self.producer = Producer("movie")  # Cola para enviar datos
        self.consumer = Consumer("result")  # Cola para recibir resultados
        self.batch_size = 10

    def wait_for_result(self, timeout: int = 60) -> bool:
        """Espera por un resultado de la cola 'result'"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            result = self.consumer.dequeue()
            if result:
                print(f"[INFO] Resultado recibido: {result}")
                return True
            time.sleep(1)
        
        print("[ERROR] Timeout esperando resultado")
        return False

    def close(self):
        """Cierra las conexiones"""
        self.producer.close()
        self.consumer.close()

    def send(self, message: dict):
        print(f"[CLIENT] Enviando mensaje: {message}")
        return self.producer.enqueue(message)

if __name__ == '__main__':
    time.sleep(10) # espera que rabbit termine de conectarse
    client = Client()

    file = open("root/files/movies.txt", "r")
    lines = file.readlines()

    header = lines[0]
    lines = lines[1:]
    batch_size = 10

    for i in range(0, len(lines), batch_size):
        batch = lines[i:i + batch_size]
        batch_str = header + ''.join(batch)
        result = client.send({"cola": batch})

        if result:
            print(f"[MAIN] Batch enviado correctamente")
        else:
            print("[MAIN] Falló el envío del batch")
        client.wait_for_result()

    time.sleep(30)
    file.close()
    client.close()

import time

from producer.producer import Producer

class Client:
    def __init__(self):
        self.producer = Producer("cola")

    def send(self, message: dict):
        print(f"[CLIENT] Enviando mensaje: {message}")
        return self.producer.enqueue(message)

    def close(self):
        self.producer.close()

if __name__ == '__main__':
    time.sleep(10) # espera que rabbit termine de conectarse
    client = Client()

    #file = open("../files/movies.txt", "r")
    #lines = file.readlines()
    #file.close()

    result = client.send({"cola": "mensaje de prueba"})

    if result:
        print(f"[MAIN] Mensaje recibido correctamente: {result}")
    else:
        print("[MAIN] No se recibió ningún mensaje")

    time.sleep(60)

    client.close()

import time

from middleware.producer.producer import Producer

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

    time.sleep(60)
    file.close()
    client.close()

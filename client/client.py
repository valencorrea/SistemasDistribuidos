import logging
from middleware.file_consuming.file_consuming import CSVSender
import os
import time

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

class Client:
    def __init__(self):
        self.sender = CSVSender(
            host=os.getenv("DECODIFIER_HOST", "localhost"),
            port=int(os.getenv("DECODIFIER_PORT", 50000))
        )
    def close(self):
        self.sender.close()
    def start(self):
        logger.info("Comenzando envío de archivos")
        success = False

        files_to_send = [
            ("/root/files/ratings.csv", "rating"),
            ("/root/files/movies_metadata.csv", "movie"),
            ("/root/files/credits.csv", "credit"),
        ]

        for retry in range(3):
            try:
                logger.debug(f"Enviando archivos (intento {retry + 1})")
                client_id = os.getenv("CLIENT_ID")
                if self.sender.send_multiple_csv(files_to_send, client_id):
                    logger.info(f"Archivos enviados exitosamente")
                    success = True
                    break
            except Exception as e:
                logger.error(f"Error enviando archivos (intento {retry + 1}): {e}")
                if retry < 2:
                    logger.info("Esperando 2 segundos antes de reintentar...")
                    time.sleep(2)

        if not success:
            logger.info("Todos los archivos enviados exitosamente")
            raise Exception(f"No se pudo enviar los archivos después de 3 intentos")

        self.sender.receive_results(expected_results=5)
        self.sender.close()


if __name__ == '__main__':
    client = Client()
    for i in range(6):
        time.sleep(i)
    client.start()
    time.sleep(10*60)

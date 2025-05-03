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

    def start(self):
        logger.info("Comenzando envío de archivos")
        max_retries = 10
        retry_delay = 10

        
        try:
            logger.info(f"Intento 1 de {max_retries} para enviar archivos")
            
            files_to_send = [
                ("root/files/movies_metadata.csv", "movie"),
                ("root/files/credits.csv", "actor"),
                ("root/files/ratings.csv", "rating")
            ]
                
            for file_path, file_type in files_to_send:
                success = False
                for retry in range(3):
                    try:
                        logger.info(f"Enviando archivo {file_path} (intento {retry + 1})")
                        if self.sender.send_csv(file_path, file_type):
                            logger.info(f"Archivo {file_path} enviado exitosamente")
                            success = True
                            break
                    except Exception as e:
                        logger.error(f"Error enviando {file_path} (intento {retry + 1}): {e}")
                        if retry < 2: 
                            logger.info("Esperando 2 segundos antes de reintentar...")
                            time.sleep(2)
                
                if not success:
                    raise Exception(f"No se pudo enviar {file_path} después de 3 intentos")
            
            logger.info("Todos los archivos enviados exitosamente")
            return 
            
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Intento {attempt + 1} fallido: {e}. Reintentando en {retry_delay} segundos...")
                time.sleep(retry_delay)
            else:
                logger.error(f"[ERROR] Error durante el procesamiento después de {max_retries} intentos: {e}")
                raise

if __name__ == '__main__':
    client = Client()
    client.start()

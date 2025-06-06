import logging
import os
import json

from middleware.file_consuming.file_consuming import CSVSender

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

        files_to_send = [
            ("/root/files/ratings.csv", "rating"),
            ("/root/files/movies_metadata.csv", "movie"),
            ("/root/files/credits.csv", "credit"),
        ]

        client_id = os.getenv("CLIENT_ID")
        try:
            if not self.sender.send_multiple_csv(files_to_send, client_id):
                logger.error("No se pudo enviar los archivos")
                exit(1)
            logger.info("Archivos enviados exitosamente")
        except Exception as e:
            logger.error(f"Error enviando archivos: {e}")
            exit(1)
        results_path = "/root/results/results.json"

        with open(results_path, "r", encoding="utf-8") as f:
            file_results = json.load(f)
            print("Parsed results as JSON:")
            print(json.dumps(file_results, indent=2, ensure_ascii=False))

            for result in self.sender.receive_results(expected_results=5):
                try:
                    parsed = json.loads(result)
                    print(f"Resultado del servidor (JSON): {parsed}")
                    result_number = parsed.get('result_number')
                    if result_number == 1:
                        if not compare_query_1(parsed, file_results["1"]):
                            logger.error("Error en la comparación de la consulta 1")
                    elif result_number == 2:
                        if not compare_query_2(parsed, file_results["2"]):
                            logger.error("Error en la comparación de la consulta 2")
                    elif result_number == 3:
                        if not compare_query_3(parsed, file_results["3"]):
                            logger.error("Error en la comparación de la consulta 3")
                    elif result_number == 4:
                        if not compare_query_4(parsed, file_results["4"]):
                            logger.error("Error en la comparación de la consulta 4")
                    elif result_number == 5:
                        if not compare_query_5(parsed, file_results["5"]):
                            logger.error("Error en la comparación de la consulta 5")
                    else:
                        logger.error(f"Resultado desconocido: {result_number}")
                        return
                    logger.info(f"Resultado {result_number} correcto!")
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON: {e}")
                    print(f"Resultado del servidor (raw): {result}")

        self.sender.close()

def compare_query_1(server, file):
    server_set = {(m['title'], tuple(sorted(m['genres']))) for m in server['result']}
    file_set = {(m['title'], tuple(sorted(eval(m['genres'])))) for m in file}
    return server_set == file_set

def compare_query_2(server, file):
    server_dict = {item['country']: item['total_budget'] for item in server['result']}
    return server_dict == file

def compare_query_3(server, file):
    best_match = (server['actors']['best']['title'] == file[0]['title'] and
                  server['actors']['best']['rating'] == file[0]['rating'])
    worst_match = (server['actors']['worst']['title'] == file[1]['title'] and
                   server['actors']['worst']['rating'] == file[1]['rating'])
    return best_match and worst_match

def compare_query_4(server, file):
    file_pairs = []
    for k in file['name']:
        file_pairs.append([file['name'][k], file['count'][k]])
    server_sorted = sorted(server['actors'], key=lambda x: (x[1], x[0]), reverse=True)
    file_sorted = sorted(file_pairs, key=lambda x: (x[1], x[0]), reverse=True)
    return server_sorted == file_sorted

def compare_query_5(server, file):
    def close(a, b, tol=1e-6):
        return abs(a - b) < tol
    return (close(server['NEGATIVE'], file['NEGATIVE']) and
            close(server['POSITIVE'], file['POSITIVE']))

if __name__ == '__main__':
    client = Client()
    client.start()

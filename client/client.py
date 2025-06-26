import json
import logging
import os

from middleware.file_consuming.file_consuming import CSVSender

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class Client:
    def __init__(self):
        self.sender = CSVSender(
            host=os.getenv("DECODIFIER_HOST", "localhost"),
            port=int(os.getenv("DECODIFIER_PORT", 50000))
        )
        self.files_to_send = [
            ("/root/files/movies_metadata.csv", "movie"),
            ("/root/files/ratings.csv", "rating"),
            ("/root/files/credits.csv", "credit"),
        ]
        self.client_id = os.getenv("CLIENT_ID")
        self.results_path = "/root/results/results.json"

    def start(self):
        logger.info("Comenzando envío de archivos")
        try:
            if not self.sender.send_multiple_csv(self.files_to_send, self.client_id):
                logger.error("No se pudo enviar los archivos")
                exit(1)
            logger.info("Archivos enviados exitosamente")
        except Exception as e:
            logger.error(f"Error enviando archivos: {e}")
            exit(1)

        with open(self.results_path, "r", encoding="utf-8") as f:
            expected = json.load(f)
            print("Parsed results as JSON:")
            print(json.dumps(expected, indent=2, ensure_ascii=False))

        for result in self.sender.receive_results(expected_results=5):
            try:
                parsed = json.loads(result)
                print(f"Resultado del servidor (JSON): {parsed}")
                self.validate_result(parsed, expected)
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON: {e}")
                print(f"Resultado del servidor (raw): {result}")

        self.sender.close()

    def validate_result(self, parsed, expected):
        number = parsed.get("result_number")

        if number == 1:
            if not self.compare_query_1(parsed, expected["1"]):
                logger.error("Error en la comparación de la consulta 1")
            else:
                logger.info("Resultado 1 correcto!")
        elif number == 2:
            if not self.compare_query_2(parsed, expected["2"]):
                logger.error("Error en la comparación de la consulta 2")
            else:
                logger.info("Resultado 2 correcto!")
        elif number == 3:
            if not self.compare_query_3(parsed, expected["3"]):
                logger.error("Error en la comparación de la consulta 3")
            else:
                logger.info("Resultado 3 correcto!")
        elif number == 4:
            if not self.compare_query_4(parsed, expected["4"]):
                logger.error("Error en la comparación de la consulta 4")
            else:
                logger.info("Resultado 4 correcto!")
        elif number == 5:
            if not self.compare_query_5(parsed, expected["5"]):
                logger.error("Error en la comparación de la consulta 5")
            else:
                logger.info("Resultado 5 correcto!")
        else:
            logger.error(f"Resultado desconocido: {number}")

    @staticmethod
    def compare_query_1(server, file):
        server_set = {(m['title'], tuple(sorted(m['genres']))) for m in server['result']}
        file_set = {(m['title'], tuple(sorted(eval(m['genres'])))) for m in file}
        return server_set == file_set

    @staticmethod
    def compare_query_2(server, file):
        return {item['country']: item['total_budget'] for item in server['result']} == file

    @staticmethod
    def compare_query_3(server, file):
        best = server['actors']['best']
        worst = server['actors']['worst']
        return (
                best['title'] == file[0]['title'] and best['rating'] == file[0]['rating'] and
                worst['title'] == file[1]['title'] and worst['rating'] == file[1]['rating']
        )

    @staticmethod
    def compare_query_4(server, file):
        file_data = [[file['name'][k], file['count'][k]] for k in file['name']]
        server_sorted = sorted(server['actors'], key=lambda x: (x[1], x[0]), reverse=True)
        file_sorted = sorted(file_data, key=lambda x: (x[1], x[0]), reverse=True)
        return server_sorted == file_sorted

    @staticmethod
    def compare_query_5(server, file):
        def nearly_equal(a, b, tol=1e-6):
            return abs(a - b) < tol

        return (
                nearly_equal(server['result']['NEGATIVE']['revenue'], file['NEGATIVE']) and
                nearly_equal(server['result']['POSITIVE']['revenue'], file['POSITIVE'])
        )


if __name__ == "__main__":
    Client().start()

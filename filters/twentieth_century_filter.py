import time

from middleware.consumer.consumer import Consumer
from utils.parsers.movie_parser import convert_data


class TwentiethCenturyFilter:
    def __init__(self, queue_name='cola'):
        self.consumer = Consumer(
            queue_name=queue_name,
            message_factory=self.handle_message
        )

    def handle_message(self, message_bytes: bytes):
        import json
        batch = json.loads(message_bytes.decode())

        print(f"[CONSUMER_CLIENT] Mensaje recibido: {batch}")


        movies = convert_data(batch)
        print(f"[CONSUMER_CLIENT] Mensaje recibido: {movies}")

        filtered_movies = apply_filter(movies)

        for movie in filtered_movies:
            print(f"[CONSUMER_CLIENT] 🎬 Pasa el filtro: {movie.title} ({movie.release_date})")

        return filtered_movies


    def start(self):
        try:
            print("[CONSUMER_CLIENT] Iniciando consumo de mensajes...")
            self.consumer.start()
        except KeyboardInterrupt:
            print("[CONSUMER_CLIENT] Interrumpido por el usuario")
            #self.close()

    #def close(self):
        #self.consumer.close()

def apply_filter(movies):
    return [movie for movie in movies if movie.released_in_or_after_2000()]


if __name__ == '__main__':
    time.sleep(5)  # Espera para asegurarse que RabbitMQ está listo
    client = TwentiethCenturyFilter()
    client.start()
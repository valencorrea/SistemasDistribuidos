from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data
from worker.filter.filter import Filter


class TwentiethCenturyFilter(Filter):
    def __init__(self):
        super().__init__("twentieth_century_filter", "movies")
        self.consumer = Consumer("movie", _message_handler=self.handle_message)
        self.producer = Producer("twentieth_century")

    def create_consumer(self):
        return Consumer("movie", _message_handler=self.handle_message)

    def create_producers(self):
        return [Producer("twentieth_century")]

    def filter(self, message):
        movies = convert_data(message)
        self.logger.info(f"Mensaje de peliculas sin filtrar recibido: {len(movies)} peliculas")
        filtered_movies = [movie for movie in movies if movie.released_in_or_after_2000()]
        self.logger.info(f"Se encontraron {len(filtered_movies)} peliculas de la decada de los 2000")
        return [movie.to_dict() for movie in filtered_movies]


if __name__ == '__main__':
    worker = TwentiethCenturyFilter()
    worker.start()

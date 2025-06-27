from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data_for_main_movie_filter
from worker.filter.filter import Filter


class MainMovieFilter(Filter):
    def __init__(self):
        super().__init__("main_movie_filter", "movies")

    def create_consumer(self):
        return Consumer("movie_main_filter", _message_handler=self.handle_message)

    def create_producers(self):
        return [Producer("movie"), Producer("movie_2"), Producer("movie_1")]

    def filter(self, message):
        movies = convert_data_for_main_movie_filter(message)
        self.logger.info(f"Se van a enviar {len(movies)} peliculas")
        return [movie.to_dict() for movie in movies]


if __name__ == '__main__':
    worker = MainMovieFilter()
    worker.start()

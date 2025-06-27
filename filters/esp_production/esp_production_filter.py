from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import parse_genres
from worker.filter.filter import Filter


class ArgEspProductionFilter(Filter):
    def __init__(self):
        super().__init__("arg_esp_production_filter", "movies")

    def create_consumer(self):
        return Consumer("arg_production", _message_handler=self.handle_message)

    def create_producers(self):
        return [Producer("aggregate_consulta_1")]

    def filter(self, message):
        movies = message.get("movies", [])
        result = []
        for movie in movies:
            if "ES" in movie.get("production_countries") and int(movie.get("release_date")) < 2010:
                result.append({"title": movie.get("title"), "genres": parse_genres(movie.get("genres"))})
        return result


if __name__ == '__main__':
    worker = ArgEspProductionFilter()
    worker.start()

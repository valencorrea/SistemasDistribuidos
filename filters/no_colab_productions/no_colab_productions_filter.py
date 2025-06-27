from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data_for_second_filter
from worker.filter.filter import Filter


class NoColabProductionsFilter(Filter):
    def __init__(self):
        super().__init__("no_colab_productions_filter", "movies")
        self.consumer = Consumer("movie_1", _message_handler=self.handle_message)
        self.producer = Producer("aggregate_consulta_2")

    def create_consumer(self):
        return Consumer("movie_1", _message_handler=self.handle_message)

    def create_producers(self):
        return [Producer("aggregate_consulta_2")]

    def filter(self, message):
        movies = convert_data_for_second_filter(message)
        result = []
        for movie in movies:
            if movie.get("production_countries") and len(movie.get("production_countries")) == 1:
                result.append({"country": movie.get("production_countries")[0], "budget": movie.get("budget")})
        return result


if __name__ == '__main__':
    worker = NoColabProductionsFilter()
    worker.start()

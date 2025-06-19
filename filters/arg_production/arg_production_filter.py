from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data
from worker.filter.filter import Filter


class ArgProductionFilter(Filter):
    def __init__(self):
        super().__init__("arg_production_filter", "movies")

    def create_consumer(self):
        return Consumer("twentieth_century", _message_handler=self.handle_message)

    def create_producers(self):
        return [Producer("arg_production"), Producer("20_century_batch_results")]

    def filter(self, message):
        movies = convert_data(message)
        filtered_movies = [movie for movie in movies if movie.angentinian_production()]
        return [movie.to_dict() for movie in filtered_movies]


if __name__ == '__main__':
    worker = ArgProductionFilter()
    worker.start()

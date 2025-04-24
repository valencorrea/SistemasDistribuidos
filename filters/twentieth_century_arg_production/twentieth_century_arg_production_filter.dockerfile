FROM python:3.9-slim
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY middleware/producer/producer.py /root/middleware/producer/producer.py
COPY filters/twentieth_century_arg_production/twentieth_century_arg_production_filter.py /root/filters/twentieth_century_arg_production/twentieth_century_arg_production_filter.py
COPY worker/worker.py /root/worker/worker.py
COPY model/movie.py /root/model/movie.py
COPY utils/parsers/movie_parser.py /root/utils/parsers/movie_parser.py
RUN pip install pika
ENV PYTHONPATH="/root"
CMD ["python", "/root/filters/twentieth_century_arg_production/twentieth_century_arg_production_filter.py"]
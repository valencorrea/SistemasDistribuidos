FROM python:3.9-slim
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY middleware/producer/producer.py /root/middleware/producer/producer.py
COPY filters/top_5_local/top_5_local.py /root/filters/top_5_local/top_5_local.py
COPY model/movie.py /root/model/movie.py
COPY worker/worker.py /root/worker/worker.py
COPY utils/parsers/movie_parser.py /root/utils/parsers/movie_parser.py
RUN pip install pika
ENV PYTHONPATH="/root"
CMD ["python", "/root/filters/top_5_local/top_5_local.py"]
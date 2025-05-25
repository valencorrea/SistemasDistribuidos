FROM python:3.9-alpine

COPY aggregator/sentiment_aggregator/aggregator.py /root/aggregator/sentiment_aggregator/aggregator.py
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY middleware/producer/producer.py /root/middleware/producer/producer.py
COPY middleware/heartbeat/heartbeat_sender.py /root/middleware/heartbeat/heartbeat_sender.py
COPY worker/worker.py /root/worker/worker.py
COPY utils/parsers/service_parser.py /root/utils/parsers/service_parser.py
RUN pip install pika
ENV PYTHONPATH="/root"

CMD ["python", "/root/aggregator/sentiment_aggregator/aggregator.py"]
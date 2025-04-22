FROM python:3.9-slim

COPY aggregator/sentiment_aggregator/aggregator.py /root/aggregator/sentiment_aggregator/aggregator.py
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY middleware/producer/producer.py /root/middleware/producer/producer.py

RUN pip install pika
ENV PYTHONPATH="/root"

CMD ["python", "/root/aggregator/sentiment_aggregator/aggregator.py"]
FROM python:3.9-slim

COPY aggregator/top_aggregator/aggregator.py /root/aggregator/top_aggregator/aggregator.py
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY middleware/producer/producer.py /root/middleware/producer/producer.py
COPY worker/worker.py /root/worker/worker.py

RUN pip install pika
ENV PYTHONPATH="/root"

CMD ["python", "/root/aggregator/top_aggregator/aggregator.py"] 
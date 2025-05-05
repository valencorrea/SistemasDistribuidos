FROM python:3.9-alpine

COPY aggregator/twentieth_century_arg_aggregator/aggregator.py /root/aggregator/twentieth_century_arg_aggregator/aggregator.py
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY middleware/producer/producer.py /root/middleware/producer/producer.py
COPY middleware/producer/publisher.py /root/middleware/producer/publisher.py
COPY worker/worker.py /root/worker/worker.py

RUN pip install pika
ENV PYTHONPATH="/root"

CMD ["python", "/root/aggregator/twentieth_century_arg_aggregator/aggregator.py"]
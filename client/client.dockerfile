FROM python:3.9-slim
COPY client/client.py /root/client/client.py
COPY consumer/consumer.py /root/consumer/consumer.py
COPY producer/producer.py /root/producer/producer.py
RUN pip install pika
ENV PYTHONPATH="/root"
CMD ["python", "/root/client/client.py"]
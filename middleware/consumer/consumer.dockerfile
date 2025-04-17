FROM python:3.9-slim
COPY consumer.py /root/consumer.py
RUN pip install pika
CMD ["python", "/root/consumer.py"]
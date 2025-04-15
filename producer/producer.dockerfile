FROM python:3.9-slim
COPY producer.py /root/producer.py
RUN pip install pika
CMD ["python", "/root/producer.py"]
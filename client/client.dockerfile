FROM python:3.9-slim
COPY client/client.py /root/client/client.py
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY middleware/producer/producer.py /root/middleware/producer/producer.py
COPY files/movies.txt /root/files/movies.txt
COPY files/credits.csv /root/files/credits.csv
RUN pip install pika
ENV PYTHONPATH="/root"
CMD ["python", "/root/client/client.py"]
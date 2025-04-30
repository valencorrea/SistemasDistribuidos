FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime
# Optional: upgrade pip
RUN pip install --upgrade pip
COPY test/integration.py /root/test/integration.py
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY middleware/producer/producer.py /root/middleware/producer/producer.py
COPY files/movies_metadata.csv /root/files/movies_metadata.csv
COPY files/credits.csv /root/files/credits.csv
COPY files/ratings.csv /root/files/ratings.csv
COPY worker/worker.py /root/worker/worker.py
RUN pip install pika transformers pandas numpy langid
ENV PYTHONPATH="/root"
CMD ["python", "/root/test/integration.py"]
FROM python:3.9-slim
COPY producer/producer.py /root/producer.py
CMD ["python", "/root/producer.py"]
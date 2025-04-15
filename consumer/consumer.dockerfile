FROM python:3.9-slim
COPY consumer/consumer.py /root/consumer.py
CMD ["python", "/root/consumer.py"]
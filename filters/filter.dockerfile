FROM python:3.9-slim
COPY consumer/consumer.py /root/consumer/consumer.py
COPY filters/twentieth_century_filter.py /root/filters/twentieth_century_filter.py
RUN pip install pika
ENV PYTHONPATH="/root"
CMD ["python", "/root/filters/twentieth_century_filter.py"]
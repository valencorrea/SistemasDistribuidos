FROM python:3.9-slim
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY filters/twentieth_century_filter.py /root/filters/twentieth_century_filter.py
RUN pip install pika
ENV PYTHONPATH="/root"
CMD ["python", "/root/filters/twentieth_century_filter.py"]
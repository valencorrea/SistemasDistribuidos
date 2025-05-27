FROM python:3.9-alpine

RUN pip install --upgrade pip
RUN pip install pika

COPY middleware /app/middleware
COPY worker/worker.py /app/worker/worker.py

ENV PYTHONPATH="/app"

WORKDIR /app
FROM python:3.9-slim

WORKDIR /app

COPY aggregator /app/aggregator
COPY worker /app/worker
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

ENV MIDDLEWARE_HOST=middleware

CMD ["python", "-m", "aggregator.result_aggregator"] 
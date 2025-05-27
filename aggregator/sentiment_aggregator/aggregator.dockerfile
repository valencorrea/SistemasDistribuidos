FROM worker:latest

COPY aggregator/sentiment_aggregator/aggregator.py /app/aggregator/sentiment_aggregator/aggregator.py

CMD ["python", "/app/aggregator/sentiment_aggregator/aggregator.py"]
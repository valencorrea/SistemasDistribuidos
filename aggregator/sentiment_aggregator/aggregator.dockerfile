FROM worker:latest

COPY aggregator/sentiment_aggregator/aggregator.py /app/aggregator/sentiment_aggregator/aggregator.py
COPY worker/abstractaggregator/abstractaggregator.py /app/worker/abstractaggregator/abstractaggregator.py

CMD ["python", "/app/aggregator/sentiment_aggregator/aggregator.py"]

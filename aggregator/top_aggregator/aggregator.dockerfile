FROM worker:latest

COPY aggregator/top_aggregator/aggregator.py /app/aggregator/top_aggregator/aggregator.py

CMD ["python", "/app/aggregator/top_aggregator/aggregator.py"] 
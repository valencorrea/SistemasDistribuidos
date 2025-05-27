FROM worker:latest

COPY aggregator/best_and_worst_ratings_aggregator/aggregator.py /app/aggregator/best_and_worst_ratings_aggregator/aggregator.py

CMD ["python", "/app/aggregator/best_and_worst_ratings_aggregator/aggregator.py"]

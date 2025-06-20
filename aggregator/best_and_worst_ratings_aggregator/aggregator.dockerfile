FROM worker:latest

COPY aggregator/best_and_worst_ratings_aggregator/aggregator.py /app/aggregator/best_and_worst_ratings_aggregator/aggregator.py
COPY worker/abstractaggregator/abstractaggregator.py /app/worker/abstractaggregator/abstractaggregator.py
COPY middleware/tcp_protocol/tcp_protocol.py /app/middleware/tcp_protocol/tcp_protocol.py
EXPOSE 60001
CMD ["python", "/app/aggregator/best_and_worst_ratings_aggregator/aggregator.py"]

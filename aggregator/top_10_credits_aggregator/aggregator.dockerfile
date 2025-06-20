FROM worker:latest

COPY aggregator/top_10_credits_aggregator/aggregator.py /app/aggregator/top_10_credits_aggregator/aggregator.py
COPY worker/abstractaggregator/abstractaggregator.py /app/worker/abstractaggregator/abstractaggregator.py
COPY middleware/tcp_protocol/tcp_protocol.py /app/middleware/tcp_protocol/tcp_protocol.py

CMD ["python", "/app/aggregator/top_10_credits_aggregator/aggregator.py"]

EXPOSE 60000

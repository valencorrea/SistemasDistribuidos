FROM worker:latest

COPY aggregator/twentieth_century_arg_aggregator/aggregator.py /app/aggregator/twentieth_century_arg_aggregator/aggregator.py
COPY worker/abstractaggregator/abstractaggregator.py /app/worker/abstractaggregator/abstractaggregator.py

CMD ["python", "/app/aggregator/twentieth_century_arg_aggregator/aggregator.py"]

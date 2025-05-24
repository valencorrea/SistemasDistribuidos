FROM worker:latest

COPY aggregator/twentieth_century_arg_aggregator/aggregator.py /app/aggregator/twentieth_century_arg_aggregator/aggregator.py

CMD ["python", "/app/aggregator/twentieth_century_arg_aggregator/aggregator.py"]
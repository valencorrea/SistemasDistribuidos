FROM worker:latest

COPY aggregator/twentieth_century_arg_esp_aggregator/aggregator.py /root/aggregator/twentieth_century_arg_esp_aggregator/aggregator.py
COPY worker/abstractaggregator/abstractaggregator.py /app/worker/abstractaggregator/abstractaggregator.py

CMD ["python", "/root/aggregator/twentieth_century_arg_esp_aggregator/aggregator.py"]
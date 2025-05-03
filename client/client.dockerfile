FROM python:3.9-slim

COPY client/client.py .

ENV PYTHONPATH="/app"

CMD ["python", "client.py"]

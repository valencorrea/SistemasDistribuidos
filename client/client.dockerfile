FROM python:3.9-alpine

COPY client/client.py .

ENV PYTHONPATH="/app"

CMD ["python", "client.py"]

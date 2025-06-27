FROM python:3.9-alpine

COPY client/client.py .
COPY middleware/file_consuming /app/middleware/file_consuming
ENV PYTHONPATH="/app"

CMD ["python", "client.py"]

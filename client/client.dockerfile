FROM python:3.9-slim

COPY client/client.py .
COPY files/movies_metadata.csv /root/files/movies_metadata.csv
COPY files/credits.csv /root/files/credits.csv
COPY files/ratings.csv /root/files/ratings.csv

COPY middleware /app/middleware/

ENV PYTHONPATH="/app"

CMD ["python", "client.py"]

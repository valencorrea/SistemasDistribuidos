FROM python:3.9-slim

COPY client/client.py .
COPY files/files_real/movies_metadata.csv /root/files/movies_metadata.csv
COPY files/files_real/credits.csv /root/files/credits.csv
COPY files/files_real/ratings.csv /root/files/ratings.csv

COPY middleware /app/middleware/

ENV PYTHONPATH="/app"

CMD ["python", "client.py"]
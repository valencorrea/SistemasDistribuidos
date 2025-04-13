FROM python:3.9-slim

WORKDIR /app

COPY worker /app/worker
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

ENV FILTER_TYPE=post_2000
ENV MIDDLEWARE_HOST=middleware

CMD ["sh", "-c", "python -c \"from worker.movie_filter_worker import MovieFilterWorker; worker = MovieFilterWorker('${FILTER_TYPE}', host='${MIDDLEWARE_HOST}'); worker.start()\""] 
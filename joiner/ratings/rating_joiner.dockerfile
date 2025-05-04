FROM python:3.9-alpine
COPY middleware/ /root/middleware/
COPY joiner/ratings/rating_joiner.py /root/joiner/ratings/rating_joiner.py
COPY utils/parsers/movie_parser.py /root/utils/parsers/movie_parser.py
COPY utils/parsers/ratings_parser.py /root/utils/parsers/ratings_parser.py
COPY worker/worker.py /root/worker/worker.py
COPY model/movie.py /root/model/movie.py
RUN pip install pika
ENV PYTHONPATH="/root"
CMD ["python", "/root/joiner/ratings/rating_joiner.py"]
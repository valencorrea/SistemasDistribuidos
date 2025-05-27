FROM worker:latest

COPY joiner/ratings/ratings_joiner.py /app/joiner/ratings/ratings_joiner.py
COPY utils/parsers/movie_parser.py /app/utils/parsers/movie_parser.py
COPY utils/parsers/ratings_parser.py /app/utils/parsers/ratings_parser.py
COPY model/movie.py /app/model/movie.py

CMD ["python", "/app/joiner/ratings/ratings_joiner.py"]

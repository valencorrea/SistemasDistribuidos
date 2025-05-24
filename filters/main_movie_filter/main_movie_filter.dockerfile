FROM worker:latest

COPY filters/main_movie_filter/main_movie_filter.py /app/filters/main_movie_filter/main_movie_filter.py
COPY model/movie.py /app/model/movie.py
COPY utils/parsers/movie_parser.py /app/utils/parsers/movie_parser.py

CMD ["python", "/app/filters/main_movie_filter/main_movie_filter.py"]
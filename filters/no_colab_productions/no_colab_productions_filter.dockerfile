FROM worker:latest

COPY filters/no_colab_productions/no_colab_productions_filter.py /app/filters/no_colab_productions/no_colab_productions_filter.py
COPY model/movie.py /app/model/movie.py
COPY utils/parsers/movie_parser.py /app/utils/parsers/movie_parser.py
COPY worker/filter/filter.py /app/worker/filter/filter.py


CMD ["python", "/app/filters/no_colab_productions/no_colab_productions_filter.py"]

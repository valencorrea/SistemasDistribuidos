FROM worker:latest


COPY filters/arg_production/arg_production_filter.py /app/filters/arg_production/arg_production_filter.py
COPY model/movie.py /app/model/movie.py
COPY utils/parsers/movie_parser.py /app/utils/parsers/movie_parser.py
COPY worker/filter/filter.py /app/worker/filter/filter.py

CMD ["python", "/app/filters/arg_production/arg_production_filter.py"]

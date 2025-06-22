FROM worker:latest

COPY joiner/ratings/ratings_joiner.py /app/joiner/ratings/ratings_joiner.py
COPY utils/parsers/movie_parser.py /app/utils/parsers/movie_parser.py
COPY utils/parsers/ratings_parser.py /app/utils/parsers/ratings_parser.py
COPY model/movie.py /app/model/movie.py
COPY joiner/base/joiner_recovery_manager.py /app/joiner/base/joiner_recovery_manager.py
COPY middleware/tcp_protocol/tcp_protocol.py /app/middleware/tcp_protocol/tcp_protocol.py
COPY worker/abstractaggregator/abstractaggregator.py /app/worker/abstractaggregator/abstractaggregator.py

CMD ["python", "/app/joiner/ratings/ratings_joiner.py"]

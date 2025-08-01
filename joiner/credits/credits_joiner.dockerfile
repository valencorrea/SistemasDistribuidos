FROM worker:latest

COPY joiner/credits/credits_joiner.py /app/joiner/credits/credits_joiner.py
COPY utils/parsers/movie_parser.py /app/utils/parsers/movie_parser.py
COPY utils/parsers/credits_parser.py /app/utils/parsers/credits_parser.py
COPY model/movie.py /app/model/movie.py
COPY model/actor.py /app/model/actor.py
COPY middleware/tcp_protocol/tcp_protocol.py /app/middleware/tcp_protocol/tcp_protocol.py
COPY worker/abstractaggregator/abstractaggregator.py /app/worker/abstractaggregator/abstractaggregator.py

CMD ["python", "/app/joiner/credits/credits_joiner.py"]
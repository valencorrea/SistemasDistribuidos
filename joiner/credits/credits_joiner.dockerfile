FROM python:3.9-alpine
COPY middleware/ /root/middleware/
COPY middleware/heartbeat/heartbeat_sender.py /root/middleware/heartbeat/heartbeat_sender.py
COPY joiner/credits/credits_joiner.py /root/joiner/credits/credits_joiner.py
COPY utils/parsers/movie_parser.py /root/utils/parsers/movie_parser.py
COPY utils/parsers/credits_parser.py /root/utils/parsers/credits_parser.py
COPY utils/parsers/service_parser.py /root/utils/parsers/service_parser.py

COPY worker/worker.py /root/worker/worker.py
COPY model/movie.py /root/model/movie.py
COPY model/actor.py /root/model/actor.py
RUN pip install pika
ENV PYTHONPATH="/root"
CMD ["python", "/root/joiner/credits/credits_joiner.py"]
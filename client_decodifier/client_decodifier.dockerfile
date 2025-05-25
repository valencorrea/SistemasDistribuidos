FROM python:3.9-alpine

COPY client_decodifier/client_decodifier.py /root/client_decodifier/client_decodifier.py
COPY middleware/ /root/middleware/
COPY middleware/heartbeat/heartbeat_sender.py /root/middleware/heartbeat/heartbeat_sender.py
COPY worker/worker.py /root/worker/worker.py
COPY utils/parsers/service_parser.py /root/utils/parsers/service_parser.py
RUN pip install pika

ENV PYTHONPATH="/root"

CMD ["python", "/root/client_decodifier/client_decodifier.py"]
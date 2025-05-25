FROM python:3.9-alpine

COPY monitor/monitor.py /root/monitor/monitor.py
COPY utils/parsers/service_parser.py /root/utils/parsers/service_parser.py

RUN pip install docker

ENV PYTHONPATH="/root"
ENV MONITOR_PORT=50001
ENV HEARTBEAT_INTERVAL=5000
ENV HEARTBEAT_TIMEOUT=15000

CMD ["python", "/root/monitor/monitor.py"]

FROM python:3.9-alpine

COPY monitor/monitor_cluster.py /root/monitor/monitor_cluster.py
COPY utils/parsers/service_parser.py /root/utils/parsers/service_parser.py

RUN pip install docker

ENV PYTHONPATH="/root"
ENV MONITOR_PORT=50001
ENV MONITOR_CLUSTER_PORT=50002
ENV HEARTBEAT_INTERVAL=5000
ENV HEARTBEAT_TIMEOUT=15000

CMD ["python", "/root/monitor/monitor_cluster.py"] 
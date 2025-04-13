FROM python:3.9-slim

WORKDIR /app

COPY middleware /app/middleware
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "middleware.tcp_middleware"] 
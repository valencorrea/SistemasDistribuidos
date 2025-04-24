FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime
# Optional: upgrade pip
RUN pip install --upgrade pip


# Set working directory
WORKDIR /root

# Copy code
COPY middleware/consumer/consumer.py /root/middleware/consumer/consumer.py
COPY middleware/producer/producer.py /root/middleware/producer/producer.py
COPY filters/sentiment_analizer/sentiment_analizer_filter.py /root/filters/sentiment_analizer/sentiment_analizer_filter.py
COPY model/movie.py /root/model/movie.py
COPY worker/worker.py /root/worker/worker.py
COPY utils/parsers/movie_parser.py /root/utils/parsers/movie_parser.py

# Install only the missing dependencies (transformers, pika)
RUN pip install pika transformers

# Set PYTHONPATH to make relative imports work
ENV PYTHONPATH="/root"

# Default command
CMD ["python", "filters/sentiment_analizer/sentiment_analizer_filter.py"]

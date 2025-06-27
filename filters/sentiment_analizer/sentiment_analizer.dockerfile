FROM pytorch/pytorch:2.7.0-cuda11.8-cudnn9-runtime

RUN pip install --upgrade pip
RUN pip install pika transformers

WORKDIR /app

COPY middleware /app/middleware
COPY worker/worker.py /app/worker/worker.py
COPY filters/sentiment_analizer/sentiment_analizer_filter.py /app/filters/sentiment_analizer/sentiment_analizer_filter.py
COPY model/movie.py /app/model/movie.py
COPY utils /app/utils
COPY utils/parsers/movie_parser.py /app/utils/parsers/movie_parser.py
COPY worker/filter/filter.py /app/worker/filter/filter.py


ENV PYTHONPATH="/app"

CMD ["python", "filters/sentiment_analizer/sentiment_analizer_filter.py"]

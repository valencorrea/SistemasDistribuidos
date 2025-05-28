FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime
# Optional: upgrade pip
RUN pip install --upgrade pip
COPY test/integration.py /root/test/integration.py
COPY middleware /root/middleware/
COPY worker/worker.py /root/worker/worker.py
COPY utils /root/utils
RUN pip install pika transformers pandas numpy langid huggingface_hub[hf_xet]
ENV PYTHONPATH="/root"
ENV PYTHONUNBUFFERED=1
CMD ["python", "/root/test/integration.py"]
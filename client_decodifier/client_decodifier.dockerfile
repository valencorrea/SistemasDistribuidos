FROM worker:latest

COPY client_decodifier/client_decodifier.py /root/client_decodifier/client_decodifier.py

CMD ["python", "/root/client_decodifier/client_decodifier.py"]
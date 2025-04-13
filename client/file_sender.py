import socket
import json
import struct
import os
from typing import Optional

class FileSender:
    def __init__(self, host: str = 'localhost', port: int = 5000, chunk_size: int = 1000):
        self.host = host
        self.port = port
        self.chunk_size = chunk_size
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
    def connect(self):
        self.socket.connect((self.host, self.port))
        # Registrarse como cliente
        self._send_message({'type': 'client'})
        
    def send_file(self, file_path: str, queue_name: str):
        total_lines = sum(1 for _ in open(file_path))
        lines_sent = 0
        
        with open(file_path, 'r') as file:
            header = file.readline()  # Leer encabezado
            chunk = []
            
            for line in file:
                chunk.append(line.strip())
                if len(chunk) >= self.chunk_size:
                    self._send_chunk(chunk, queue_name, total_lines, lines_sent)
                    lines_sent += len(chunk)
                    chunk = []
                    
            if chunk:  # Enviar último chunk si existe
                self._send_chunk(chunk, queue_name, total_lines, lines_sent)
                lines_sent += len(chunk)
                
    def _send_chunk(self, chunk: list, queue_name: str, total_lines: int, lines_sent: int):
        message = {
            'type': 'file_chunk',
            'target_queue': queue_name,
            'data': chunk,
            'total_lines': total_lines,
            'lines_sent': lines_sent,
            'chunk_size': len(chunk)
        }
        self._send_message(message)
        
    def _send_message(self, message: dict):
        try:
            data = json.dumps(message).encode()
            length = struct.pack('!I', len(data))
            self.socket.sendall(length + data)
        except Exception as e:
            print(f"Error enviando mensaje: {e}")
            raise
            
    def close(self):
        self.socket.close()

if __name__ == '__main__':
    sender = FileSender()
    try:
        sender.connect()
        
        # Enviar archivos en orden
        files = [
            ('movies_metadata.csv', 'movies_queue'),
            ('credits.csv', 'credits_queue'),
            ('ratings.csv', 'ratings_queue')
        ]
        
        for file_name, queue in files:
            file_path = os.path.join('data', file_name)
            if os.path.exists(file_path):
                print(f"Enviando archivo: {file_name}")
                sender.send_file(file_path, queue)
                print(f"Archivo {file_name} enviado completamente")
            else:
                print(f"Archivo no encontrado: {file_path}")
                
    finally:
        sender.close() 
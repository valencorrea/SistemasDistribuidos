import socket
import json
import struct
from abc import ABC, abstractmethod

class BaseWorker(ABC):
    def __init__(self, queue_name: str, host: str = 'localhost', port: int = 5000):
        self.queue_name = queue_name
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.is_running = True
        self.processed_lines = 0
        self.total_lines = None
        self.results = []
        
    def connect(self):
        self.socket.connect((self.host, self.port))
        # Registrarse como worker
        self._send_message({
            'type': 'register',
            'queue': self.queue_name
        })
        
    def start(self):
        self.connect()
        try:
            while self.is_running:
                message = self._receive_message()
                if message.get('type') == 'file_chunk':
                    self._process_chunk(message)
        finally:
            self.socket.close()
            
    def _process_chunk(self, message: dict):
        chunk_data = message.get('data', [])
        self.total_lines = message.get('total_lines')
        chunk_size = len(chunk_data)
        
        # Procesar datos
        processed_data = self.process_chunk(chunk_data)
        
        # Actualizar contador de líneas procesadas
        self.processed_lines += chunk_size
        
        # Enviar resultados
        self._send_results(processed_data)
            
    def _send_results(self, results):
        message = {
            'type': 'result',
            'target_queue': self.get_target_queue(),
            'data': results,
            'total_lines': self.total_lines,
            'processed_lines': self.processed_lines
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
            
    def _receive_message(self) -> dict:
        length_data = self.socket.recv(4)
        if not length_data:
            raise ConnectionError("Conexión cerrada")
            
        length = struct.unpack('!I', length_data)[0]
        data = b''
        while len(data) < length:
            chunk = self.socket.recv(min(length - len(data), 4096))
            if not chunk:
                raise ConnectionError("Conexión cerrada durante recepción")
            data += chunk
            
        return json.loads(data.decode())
        
    @abstractmethod
    def process_chunk(self, chunk_data: list) -> list:
        """Procesa un chunk de datos y retorna los resultados"""
        pass
        
    @abstractmethod
    def get_target_queue(self) -> str:
        """Retorna el nombre de la cola destino para los resultados"""
        pass 
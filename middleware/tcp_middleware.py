import socket
import json
import threading
from typing import Dict, List
import struct

class TCPMiddleware:
    def __init__(self, host: str = 'localhost', port: int = 5000):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.workers: Dict[str, List[socket.socket]] = {}
        self.worker_status: Dict[socket.socket, Dict] = {}
        self.is_running = True
        
    def start_server(self):
        self.socket.bind((self.host, self.port))
        self.socket.listen(10)
        print(f"Middleware escuchando en {self.host}:{self.port}")
        
        while self.is_running:
            client_socket, address = self.socket.accept()
            threading.Thread(target=self._handle_connection, args=(client_socket,)).start()
            
    def _handle_connection(self, client_socket: socket.socket):
        try:
            msg_type = self._receive_message(client_socket)
            if msg_type.get('type') == 'register':
                self._handle_registration(client_socket, msg_type)
            elif msg_type.get('type') == 'client':
                self._handle_client(client_socket)
        except Exception as e:
            print(f"Error manejando conexión: {e}")
            client_socket.close()
            
    def _handle_registration(self, client_socket: socket.socket, msg: dict):
        queue_name = msg.get('queue')
        if queue_name not in self.workers:
            self.workers[queue_name] = []
        self.workers[queue_name].append(client_socket)
        self.worker_status[client_socket] = {'processed_lines': 0}
        print(f"Worker registrado para cola: {queue_name}")
        
        threading.Thread(target=self._handle_worker_messages, 
                       args=(client_socket, queue_name)).start()
        
    def _handle_worker_messages(self, worker_socket: socket.socket, queue_name: str):
        while self.is_running:
            try:
                message = self._receive_message(worker_socket)
                if message.get('type') == 'result':
                    self._update_worker_status(worker_socket, message)
                    if message.get('target_queue'):
                        self._route_message(message, queue_name)
            except:
                if worker_socket in self.workers.get(queue_name, []):
                    self.workers[queue_name].remove(worker_socket)
                if worker_socket in self.worker_status:
                    del self.worker_status[worker_socket]
                worker_socket.close()
                break
                
    def _update_worker_status(self, worker_socket: socket.socket, message: dict):
        if worker_socket in self.worker_status:
            self.worker_status[worker_socket]['processed_lines'] = message.get('processed_lines', 0)
                
    def _handle_client(self, client_socket: socket.socket):
        while self.is_running:
            try:
                message = self._receive_message(client_socket)
                if message.get('type') == 'file_chunk':
                    queue_name = message.get('target_queue')
                    if queue_name in self.workers:
                        self._distribute_chunk(message, queue_name)
            except:
                client_socket.close()
                break
                
    def _distribute_chunk(self, message: dict, queue_name: str):
        if not self.workers.get(queue_name):
            return
            
        # Encontrar el worker que ha procesado menos líneas
        worker_socket = min(
            self.workers[queue_name],
            key=lambda w: self.worker_status.get(w, {}).get('processed_lines', 0)
        )
        self._send_message(worker_socket, message)
        
    def _route_message(self, message: dict, source_queue: str):
        target_queue = message.get('target_queue')
        if target_queue and target_queue in self.workers:
            for worker in self.workers[target_queue]:
                self._send_message(worker, message)
            
    def _send_message(self, sock: socket.socket, message: dict):
        try:
            data = json.dumps(message).encode()
            length = struct.pack('!I', len(data))
            sock.sendall(length + data)
        except Exception as e:
            print(f"Error enviando mensaje: {e}")
            
    def _receive_message(self, sock: socket.socket) -> dict:
        length_data = sock.recv(4)
        if not length_data:
            raise ConnectionError("Conexión cerrada")
            
        length = struct.unpack('!I', length_data)[0]
        data = b''
        while len(data) < length:
            chunk = sock.recv(min(length - len(data), 4096))
            if not chunk:
                raise ConnectionError("Conexión cerrada durante recepción")
            data += chunk
            
        return json.loads(data.decode())
        
    def stop(self):
        self.is_running = False
        self.socket.close()
        for workers in self.workers.values():
            for worker in workers:
                worker.close()

if __name__ == '__main__':
    middleware = TCPMiddleware()
    try:
        middleware.start_server()
    except KeyboardInterrupt:
        middleware.stop() 
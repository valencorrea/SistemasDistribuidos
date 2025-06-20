import socket
import time
import logging
import threading
import json

logger = logging.getLogger(__name__)

class TCPServer:
    def __init__(self, host, port, message_handler_callback):
        self.host = host
        self.port = port
        self.message_handler = message_handler_callback
        self._server_socket = None
        self._running = False

    def start(self):
        try:
            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_socket.bind((self.host, self.port))
            self._server_socket.listen(5)
            logger.info(f"[TCP Server] Escuchando en {self.host}:{self.port}")
            self._running = True
            
            server_thread = threading.Thread(target=self._accept_connections)
            server_thread.daemon = True
            server_thread.start()
            return True
        except Exception as e:
            logger.error(f"[TCP Server] Error iniciando: {e}")
            self._running = False
            return False

    def _accept_connections(self):
        if not self._server_socket:
            return
        while self._running:
            try:
                client_socket, addr = self._server_socket.accept()
                logger.info(f"[TCP Server] Conexión aceptada de {addr}")
                client_thread = threading.Thread(target=self._handle_client, args=(client_socket, addr))
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                if self._running:
                    logger.error(f"[TCP Server] Error aceptando conexiones: {e}")
                break
    
    def _handle_client(self, client_socket, addr):
        try:
            buffer = b""
            while True:
                data = client_socket.recv(4096)
                if not data:
                    logger.info(f"[TCP Server] Conexión cerrada por {addr}")
                    break
                buffer += data
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    msg = line.decode('utf-8').strip()
                    if msg and self.message_handler:
                        self.message_handler(msg, addr)
        except (ConnectionResetError, BrokenPipeError):
            logger.warning(f"[TCP Server] Conexión perdida con {addr}")
        except Exception as e:
            logger.error(f"[TCP Server] Error manejando cliente {addr}: {e}")
        finally:
            client_socket.close()

    def stop(self):
        self._running = False
        if self._server_socket:
            self._server_socket.close()
        logger.info("[TCP Server] Servidor detenido.")

class TCPClient:
    def __init__(self, host, port, timeout=5, max_retries=5):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries
        self._socket = None
        self.connect()

    def connect(self):
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"[TCP Client] Intentando conectar a {self.host}:{self.port} (intento {attempt+1})")
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.settimeout(self.timeout)
                self._socket.connect((self.host, self.port))
                logger.info(f"[TCP Client] Conexión establecida con {self.host}:{self.port}")
                return True
            except socket.gaierror as e:
                logger.error(f"[TCP Client] Error de DNS: {e}. Host: {self.host}")
            except Exception as e:
                logger.error(f"[TCP Client] Error conectando: {e}")
            
            time.sleep(2) # Esperar antes de reintentar
            
        self._socket = None
        logger.error(f"[TCP Client] No se pudo conectar tras {self.max_retries} intentos.")
        return False

    def send(self, message):
        if not self._socket:
            logger.error("[TCP Client] No hay conexión para enviar mensaje.")
            if not self.connect():
                return False
        
        if not self._socket: # Re-check after trying to connect
             return False

        try:
            self._socket.sendall(message.encode('utf-8'))
            return True
        except (BrokenPipeError, ConnectionResetError):
            logger.warning("[TCP Client] Conexión perdida. Intentando reconectar...")
            if self.connect() and self._socket:
                try:
                    self._socket.sendall(message.encode('utf-8'))
                    return True
                except Exception as e:
                    logger.error(f"[TCP Client] Error enviando tras reconexión: {e}")
            return False
        except Exception as e:
            logger.error(f"[TCP Client] Error inesperado al enviar: {e}")
            return False

    def close(self):
        if self._socket:
            self._socket.close()
            self._socket = None
        logger.info("[TCP Client] Conexión cerrada.")

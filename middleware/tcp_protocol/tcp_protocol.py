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
        self._client_connections = {} 

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
                
                self._client_connections[addr] = client_socket
                
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
                        self.message_handler(msg, addr, client_socket)
        except (ConnectionResetError, BrokenPipeError):
            logger.warning(f"[TCP Server] Conexión perdida con {addr}")
        except Exception as e:
            logger.error(f"[TCP Server] Error manejando cliente {addr}: {e}")
        finally:
            if addr in self._client_connections:
                del self._client_connections[addr]
            client_socket.close()

    def send_response(self, addr, response_data):
        try:
            if addr in self._client_connections:
                client_socket = self._client_connections[addr]
                response_message = json.dumps(response_data) + '\n'
                client_socket.sendall(response_message.encode('utf-8'))
                logger.info(f"[TCP Server] Respuesta enviada a {addr}: {response_data}")
                return True
            else:
                logger.warning(f"[TCP Server] No hay conexión activa para {addr}")
                return False
        except Exception as e:
            logger.error(f"[TCP Server] Error enviando respuesta a {addr}: {e}")
            return False

    def stop(self):
        self._running = False
        for addr, client_socket in self._client_connections.items():
            try:
                client_socket.close()
            except Exception:
                pass
        self._client_connections.clear()
        
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
        self._response_callbacks = {}
        self._response_thread = None
        self._running = False
        self._listener_started = False 

    def connect(self):
        if self._socket:
            try:
                return True
                #self._socket.close()
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
            
            time.sleep(5 * attempt)
            
        self._socket = None
        logger.error(f"[TCP Client] No se pudo conectar tras {self.max_retries} intentos.")
        return False

    def _start_response_listener(self):
        """Inicia thread para escuchar respuestas del servidor"""
        if self._response_thread and self._response_thread.is_alive():
            return
        
        self._running = True
        self._response_thread = threading.Thread(target=self._listen_for_responses)
        self._response_thread.daemon = True
        self._response_thread.start()
        logger.info("[TCP Client] Thread de escucha de respuestas iniciado")

    def _listen_for_responses(self):
        """Escucha respuestas del servidor"""
        buffer = b""
        while self._running and self._socket:
            try:
                data = self._socket.recv(4096)
                if not data:
                    logger.info("[TCP Client] Conexión cerrada por el servidor")
                    break
                
                buffer += data
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    msg = line.decode('utf-8').strip()
                    if msg:
                        self._handle_response(msg)
                        
            except socket.timeout:
                continue
            except (ConnectionResetError, BrokenPipeError):
                logger.warning("[TCP Client] Conexión perdida con el servidor")
                break
            except Exception as e:
                logger.error(f"[TCP Client] Error escuchando respuestas: {e}")
                break
        
        self._running = False
        logger.info("[TCP Client] Thread de escucha de respuestas terminado")

    def _handle_response(self, response_msg):
        """Maneja una respuesta recibida del servidor"""
        try:
            response_data = json.loads(response_msg)
            response_type = response_data.get("type")
            
            logger.info(f"[TCP Client] Respuesta recibida: {response_type}")
            
            if response_type in self._response_callbacks:
                callback = self._response_callbacks[response_type]
                callback(response_data)
            else:
                logger.warning(f"[TCP Client] No hay callback registrado para respuesta tipo: {response_type}")
                
        except json.JSONDecodeError:
            logger.error(f"[TCP Client] Respuesta no es JSON válido: {response_msg}")
        except Exception as e:
            logger.error(f"[TCP Client] Error procesando respuesta: {e}")

    def send_with_response(self, message, callback):
        while True:
            try:
                if not self._socket:
                    logger.info(f"[TCP Client] Intentando conectar a {self.host}:{self.port} para enviar mensaje")
                    if not self.connect():
                        logger.warning(f"[TCP Client] No se pudo conectar a {self.host}:{self.port}, reintentando en 3 segundos...")
                        time.sleep(3)
                        continue

                if not self.send(message):
                    logger.warning(f"[TCP Client] Error enviando mensaje, reintentando conexión...")
                    continue

                buffer = ""
                while True:
                    try:
                        if not self._socket:
                            logger.warning(f"[TCP Client] Socket perdido, reintentando conexión...")
                            break

                        data = self._socket.recv(1024)
                        if not data:
                            logger.warning(f"[TCP Client] Conexión cerrada por el servidor, reintentando...")
                            self._socket = None
                            break

                        buffer += data.decode('utf-8')
                        if '\n' in buffer:
                            message_end = buffer.find('\n')
                            complete_message = buffer[:message_end]
                            buffer = buffer[message_end + 1:]
                            if complete_message.strip():
                                return callback(json.loads(complete_message))
                    except socket.timeout:
                        continue
                    except (ConnectionResetError, BrokenPipeError):
                        logger.warning(f"[TCP Client] Conexión perdida, reintentando...")
                        self._socket = None
                        break
                    except Exception as e:
                        logger.error(f"[TCP Client] Error recibiendo respuesta: {e}")
                        self._socket = None
                        break

                continue

            except Exception as e:
                logger.error(f"[TCP Client] Error en send_with_response: {e}")
                self._socket = None
                time.sleep(3)
                continue

    def send(self, message):
        if not self._socket:
            logger.error("[TCP Client] No hay conexión para enviar mensaje.")
            if not self.connect():
                return False
        
        if not self._socket:
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
        self._running = False
        if self._socket:
            self._socket.close()
            self._socket = None
        logger.info("[TCP Client] Conexión cerrada.")

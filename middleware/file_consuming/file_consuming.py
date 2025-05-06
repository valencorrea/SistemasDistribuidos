import socket
import logging
from typing import Generator, Optional, Tuple, List
from dataclasses import dataclass
import time
import os

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

@dataclass
class CSVMetadata:
    name: str
    type: str  # 'movie', 'actor', 'rating'
    has_header: bool = True

class CSVSender:
    def __init__(self, host: str = "localhost", port: int = 50000):
        self.host = host
        self.port = port
        self.socket = None
        logger.info(f"CSVSender inicializado con host={host}, port={port}")

    def connect(self) -> bool:
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Intentando conectar a {self.host}:{self.port} (intento {attempt + 1})")
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((self.host, self.port))
                logger.info(f"Conexión establecida exitosamente con {self.host}:{self.port}")
                return True
            except Exception as e:
                logger.error(f"Error conectando (intento {attempt + 1}): {e}")
                if self.socket:
                    self.socket.close()
                self.socket = None
                if attempt < max_retries - 1:
                    logger.info(f"Reintentando en {retry_delay} segundos...")
                    time.sleep(retry_delay)
        return False

    def _send_all(self, data: bytes) -> bool:
        total_sent = 0
        while total_sent < len(data):
            sent = self.socket.send(data[total_sent:])
            if sent == 0:
                return False
            total_sent += sent
        return True

    def _send_line(self, line: str) -> bool:
        if not line.endswith('\n'):
            line += '\n'
        line_bytes = line.encode('utf-8')
        length = len(line_bytes)
        length_bytes = str(length).zfill(10).encode('utf-8')
        if not self._send_all(length_bytes):
            return False
        return self._send_all(line_bytes)

    def send_multiple_csv(self, file_list: List[Tuple[str, str]]) -> bool:
        if not self.connect():
            logger.error("No se pudo establecer conexión con el servidor")
            return False

        try:
            logger.info("Enviando archivos CSV...")
            for file_path, file_type in file_list:
                if not self.enviar_archivo(file_path, file_type):
                    logger.error(f"Fallo al enviar archivo: {file_path}")
                    return False

            if not self._send_line("ALL_EOF"):
                logger.error("Error enviando ALL_EOF")
                return False

            return True

        except Exception as e:
            logger.error(f"[ERROR] Error durante envío múltiple: {e}")
            return False

    def enviar_archivo(self, file_path, file_type) -> bool:
        logger.info(f"Enviando archivo {file_path} de tipo {file_type}")

        metadata = CSVMetadata(name=os.path.basename(file_path), type=file_type)
        metadata_str = f"{metadata.name}|{metadata.type}"
        if not self._send_line(metadata_str):
            logger.error(f"Error enviando metadata de {file_path}")
            return False

        with open(file_path, 'r') as file:
            for line in file:
                if not self._send_line(line.strip()):
                    logger.error(f"Error enviando línea en {file_path}")
                    return False

        if not self._send_line("EOF"):
            logger.error(f"Error enviando EOF de {file_path}")
            return False

        logger.info(f"Archivo {file_path} enviado exitosamente")
        return True


class CSVReceiver:
    def __init__(self, host: str = "0.0.0.0", port: int = 50000):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.header: Optional[str] = None

    def start_server(self) -> bool:
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.bind((self.host, self.port))
            self.socket.listen(5)
            logger.info(f"Servidor escuchando en {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Error iniciando servidor: {e}")
            return False

    def _recv_all(self, sock: socket.socket, n: int) -> bytes:
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return bytes()
            data.extend(packet)
        return bytes(data)

    def _recv_line(self, sock: socket.socket) -> Optional[str]:
        try:
            length_bytes = self._recv_all(sock, 10)
            if not length_bytes:
                return None
            length = int(length_bytes.decode('utf-8'))
            line_bytes = self._recv_all(sock, length)
            if not line_bytes:
                return None
            return line_bytes.decode('utf-8').strip()
        except Exception as e:
            logger.error(f"Error recibiendo línea: {e}")
            return None

    def process_connection(self, client_socket) -> Generator[Tuple[List[str], bool, CSVMetadata], None, None]:
        try:
            while True:
                metadata_line = self._recv_line(client_socket)
                if not metadata_line:
                    logger.error("No se recibió metadata")
                    break

                if metadata_line == "ALL_EOF":
                    logger.info("Fin de transmisión de todos los archivos")
                    break

                name, file_type = metadata_line.split('|')
                metadata = CSVMetadata(name, file_type)
                current_batch = []
                line_count = 0

                while True:
                    line = self._recv_line(client_socket)
                    if not line:
                        logger.error("Error recibiendo línea")
                        return

                    if line == "EOF":
                        logger.info(f"Fin de archivo '{name}' recibido")
                        if current_batch:
                            logger.info(f"Enviando último batch de {len(current_batch)} líneas")
                            yield current_batch, True, metadata
                        break

                    line_count += 1
                    current_batch.append(line)

                    if len(current_batch) >= 1000:
                        yield current_batch, False, metadata
                        current_batch = []

            
        except Exception as e:
            logger.error(f"Error procesando conexión: {e}")

    def accept_connection(self) -> Tuple[Optional[socket.socket], Optional[str]]:
        try:
            client_socket, addr = self.socket.accept()
            return client_socket, addr[0]
        except Exception as e:
            logger.error(f"Error aceptando conexión: {e}")
            return None, None

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

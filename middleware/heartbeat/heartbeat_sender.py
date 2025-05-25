import socket
import json
import time
import threading
import os
import logging
from utils.parsers.service_parser import ServiceParser

logger = logging.getLogger(__name__)

class HeartbeatSender:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.monitor_host = os.getenv('MONITOR_HOST', 'monitor')
        self.monitor_port = int(os.getenv('MONITOR_PORT', 50001))
        self.heartbeat_interval = int(os.getenv('HEARTBEAT_INTERVAL', 5000))  # en milisegundos
        self.socket = None
        self.running = False
        self.thread = None
    
    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._send_heartbeats)
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"Heartbeat sender iniciado para {self.service_name}")
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        if self.socket:
            self.socket.close()
        logger.info(f"Heartbeat sender detenido para {self.service_name}")
    
    def _send_heartbeats(self):
        while self.running:
            try:
                if not self.socket:
                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.socket.connect((self.monitor_host, self.monitor_port))
                
                heartbeat = ServiceParser.create_heartbeat(self.service_name)
                self.socket.send(heartbeat.encode())
                logger.debug(f"Heartbeat enviado por {self.service_name}")
                
            except Exception as e:
                logger.error(f"Error enviando heartbeat desde {self.service_name}: {e}")
                if self.socket:
                    self.socket.close()
                    self.socket = None
            
            time.sleep(self.heartbeat_interval / 1000)  # convertir a segundos
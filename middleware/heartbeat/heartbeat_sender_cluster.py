import socket
import json
import time
import threading
import os
import logging
import random
from utils.parsers.service_parser import ServiceParser

logger = logging.getLogger(__name__)

class HeartbeatSenderCluster:
    def __init__(self, service_name: str):
        self.service_name = service_name
        
        # Configurar hosts y puertos de monitores
        monitor_hosts_str = os.getenv('MONITOR_HOSTS', 'monitor_1,monitor_2,monitor_3')
        monitor_ports_str = os.getenv('MONITOR_PORTS', '50001,50002,50003')
        
        hosts = [host.strip() for host in monitor_hosts_str.split(',')]
        ports = [int(port.strip()) for port in monitor_ports_str.split(',')]
        
        # Crear lista de (host, puerto) para cada monitor
        self.monitor_endpoints = list(zip(hosts, ports))
        
        self.heartbeat_interval = int(os.getenv('HEARTBEAT_INTERVAL', 5000))  # en milisegundos
        
        # Estado de conexión
        self.current_monitor_index = 0
        self.socket = None
        self.running = False
        self.thread = None
        
        # Randomizar el orden inicial para distribución de carga
        random.shuffle(self.monitor_endpoints)
        
        logger.info(f"Heartbeat sender cluster iniciado para {self.service_name}")
        logger.info(f"Monitores disponibles: {self.monitor_endpoints}")
    
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
    
    def _get_current_monitor(self):
        """Obtiene el monitor actual basado en el índice"""
        return self.monitor_endpoints[self.current_monitor_index]
    
    def _switch_to_next_monitor(self):
        """Cambia al siguiente monitor en la lista"""
        self.current_monitor_index = (self.current_monitor_index + 1) % len(self.monitor_endpoints)
        current_host, current_port = self._get_current_monitor()
        logger.info(f"Switching to monitor: {current_host}:{current_port}")
        return self._get_current_monitor()
    
    def _connect_to_monitor(self):
        """Intenta conectarse al monitor actual, con failover si es necesario"""
        max_attempts = len(self.monitor_endpoints)
        
        for attempt in range(max_attempts):
            current_host, current_port = self._get_current_monitor()
            
            try:
                if self.socket:
                    self.socket.close()
                
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.settimeout(3)  # Timeout de 3 segundos para conexión
                self.socket.connect((current_host, current_port))
                
                logger.info(f"Connected to monitor: {current_host}:{current_port}")
                return True
                
            except Exception as e:
                logger.warning(f"Failed to connect to monitor {current_host}:{current_port}: {e}")
                self.socket = None
                
                # Cambiar al siguiente monitor si no es el último intento
                if attempt < max_attempts - 1:
                    self._switch_to_next_monitor()
        
        logger.error("Failed to connect to any monitor")
        return False
    
    def _send_heartbeats(self):
        connection_failures = 0
        max_connection_failures = 3
        
        while self.running:
            try:
                # Verificar/establecer conexión
                if not self.socket:
                    if not self._connect_to_monitor():
                        connection_failures += 1
                        if connection_failures >= max_connection_failures:
                            logger.error(f"Too many connection failures ({connection_failures}), backing off")
                            time.sleep(10)  # Back off por 10 segundos
                            connection_failures = 0
                        else:
                            time.sleep(2)  # Esperar 2 segundos antes de reintentar
                        continue
                    
                    # Reset counter en conexión exitosa
                    connection_failures = 0
                
                # Enviar heartbeat
                heartbeat = ServiceParser.create_heartbeat(self.service_name)
                self.socket.send(heartbeat.encode())
                current_host, current_port = self._get_current_monitor()
                # logger.debug(f"Heartbeat enviado por {self.service_name} a {current_host}:{current_port}")
                
            except (socket.error, ConnectionResetError, BrokenPipeError) as e:
                logger.warning(f"Connection error sending heartbeat from {self.service_name}: {e}")
                
                # Cerrar socket actual
                if self.socket:
                    self.socket.close()
                    self.socket = None
                
                # Intentar siguiente monitor
                self._switch_to_next_monitor()
                connection_failures += 1
                
            except Exception as e:
                logger.error(f"Unexpected error sending heartbeat from {self.service_name}: {e}")
                if self.socket:
                    self.socket.close()
                    self.socket = None
                connection_failures += 1
            
            time.sleep(self.heartbeat_interval / 1000)  # convertir a segundos
    
    def get_connection_status(self):
        """Devuelve el estado de la conexión actual"""
        current_monitor = None
        if self.socket:
            current_host, current_port = self._get_current_monitor()
            current_monitor = f"{current_host}:{current_port}"
            
        return {
            'connected': self.socket is not None,
            'current_monitor': current_monitor,
            'available_monitors': [f"{host}:{port}" for host, port in self.monitor_endpoints]
        } 
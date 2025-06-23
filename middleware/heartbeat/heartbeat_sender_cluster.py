import socket
import time
import threading
import os
import logging
import random
import json
from utils.parsers.service_parser import ServiceParser

logger = logging.getLogger(__name__)

class HeartbeatSenderCluster:
    def __init__(self, service_name: str):
        self.service_name = service_name
        
        monitor_hosts_str = os.getenv('MONITOR_HOSTS', 'monitor_1,monitor_2,monitor_3')
        monitor_ports_str = os.getenv('MONITOR_PORTS', '50001,50002,50003')
        
        hosts = [host.strip() for host in monitor_hosts_str.split(',')]
        ports = [int(port.strip()) for port in monitor_ports_str.split(',')]
        
        self.monitor_endpoints = list(zip(hosts, ports))
        
        self.heartbeat_interval = int(os.getenv('HEARTBEAT_INTERVAL', 5000))  # en milisegundos
        
        self.current_monitor_index = 0
        self.socket = None
        self.running = False
        self.thread = None
        
        random.shuffle(self.monitor_endpoints)
        
        logger.info(f"Heartbeat sender cluster iniciado para {self.service_name}")
        logger.info(f"Monitores disponibles: {self.monitor_endpoints}")
    
    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._send_heartbeats)
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"💓 Heartbeat sender iniciado para {self.service_name}")
        logger.info(f"🔍 Intentando conectar a monitores: {self.monitor_endpoints}")
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        if self.socket:
            self.socket.close()
        logger.info(f"🛑 Heartbeat sender detenido para {self.service_name}")
    
    def _get_current_monitor(self):
        return self.monitor_endpoints[self.current_monitor_index]
    
    def _switch_to_next_monitor(self):
        self.current_monitor_index = (self.current_monitor_index + 1) % len(self.monitor_endpoints)
        current_host, current_port = self._get_current_monitor()
        logger.info(f"🔄 Switching to monitor: {current_host}:{current_port}")
        return self._get_current_monitor()
    
    def _find_leader(self):
        """Encuentra quién es el líder actual preguntando a todos los monitores"""
        for host, port in self.monitor_endpoints:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((host, port))
                
                msg = {"type": "who_is_leader"}
                s.send(json.dumps(msg).encode())
                
                response = s.recv(1024)
                if response:
                    response_data = json.loads(response.decode())
                    leader_id = response_data.get("leader_id")
                    
                    if leader_id is not None:
                        logger.info(f"✅ Encontrado líder: {leader_id} en {host}:{port}")
                        s.close()
                        return leader_id
                s.close()
            except Exception as e:
                logger.debug(f"❌ No se pudo preguntar a {host}:{port}: {e}")
                continue
        
        logger.warning(f"❌ No se pudo encontrar líder en ningún monitor")
        return None
    
    def _connect_to_monitor(self):
        # Con UDP no necesitamos "conectar", solo crear el socket
        try:
            if self.socket:
                self.socket.close()
            
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.settimeout(1)
            
            current_host, current_port = self._get_current_monitor()
            logger.info(f"🔍 Configurado para enviar UDP a {current_host}:{current_port}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error configurando socket UDP: {e}")
            self.socket = None
            return False
    
    def _send_heartbeats(self):
        connection_failures = 0
        max_connection_failures = 5
        leader_host = None
        leader_port = None
        leader_id = None
        last_leader_check = 0
        leader_check_interval = 3000
        
        while self.running:
            try:
                if not self.socket:
                    if not self._connect_to_monitor():
                        connection_failures += 1
                        if connection_failures >= max_connection_failures:
                            logger.error(f"⚠️ Too many connection failures ({connection_failures}), backing off")
                            time.sleep(10)
                            connection_failures = 0
                        else:
                            time.sleep(2)
                        continue
                    connection_failures = 0
                

                current_time = time.time() * 1000
                if leader_id is None or (current_time - last_leader_check) >= leader_check_interval:
                    leader_id = self._find_leader()
                    leader_host = None
                    leader_port = None
                    if leader_id is not None:
                        for host, port in self.monitor_endpoints:
                            if host.endswith(str(leader_id)) or host == f"monitor_{leader_id}":
                                leader_host, leader_port = host, port
                                logger.info(f"💡 Enviando heartbeats al líder {leader_host}:{leader_port}")
                                break
                        else:
                            logger.warning(f"No se encontró el host del líder {leader_id} en la lista de monitores")
                            leader_host, leader_port = None, None
                    else:
                        logger.warning(f"No se pudo determinar el líder, reintentando en el próximo ciclo")
                        time.sleep(2)
                        continue
                    last_leader_check = current_time
                
                if leader_host and leader_port:
                    heartbeat = ServiceParser.create_heartbeat(self.service_name)
                    self.socket.sendto(heartbeat.encode(), (leader_host, leader_port))
                    logger.debug(f"💓 Heartbeat UDP enviado por {self.service_name} a {leader_host}:{leader_port}")
                else:
                    logger.warning(f"No hay líder disponible para enviar heartbeat")
                
            except Exception as e:
                logger.error(f"💥 Unexpected error sending heartbeat from {self.service_name}: {e}")
                if self.socket:
                    self.socket.close()
                    self.socket = None
                connection_failures += 1
            
            time.sleep(self.heartbeat_interval / 1000)

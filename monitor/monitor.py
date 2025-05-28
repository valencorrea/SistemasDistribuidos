import socket
import threading
import time
import json
import os
import docker
import logging
from typing import Dict

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ServiceMonitor:
    def __init__(self):
        self.port = int(os.getenv('MONITOR_PORT', 50001))
        self.heartbeat_interval = int(os.getenv('HEARTBEAT_INTERVAL', 5000))  # en milisegundos
        self.heartbeat_timeout = int(os.getenv('HEARTBEAT_TIMEOUT', 15000))   # en milisegundos
        self.services: Dict[str, float] = {}  # nombre_servicio -> último_heartbeat
        self.lock = threading.Lock()
        self.docker_client = docker.from_env()
        
    def start(self):
        # Iniciar el servidor TCP
        server_thread = threading.Thread(target=self._start_server)
        server_thread.daemon = True
        server_thread.start()
        
        # Iniciar el checker de heartbeats
        checker_thread = threading.Thread(target=self._check_heartbeats)
        checker_thread.daemon = True
        checker_thread.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Monitor shutting down...")
    
    def _start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('0.0.0.0', self.port))
        server_socket.listen(5)
        logger.info(f"Monitor listening on port {self.port}")
        
        while True:
            try:
                client_socket, address = server_socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                logger.error(f"Error accepting connection: {e}")
    
    def _handle_client(self, client_socket: socket.socket, address: tuple):
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                    
                try:
                    message = json.loads(data.decode())
                    service_name = message.get('service_name')
                    if service_name:
                        with self.lock:
                            self.services[service_name] = time.time() * 1000  # timestamp en milisegundos
                            logger.debug(f"Received heartbeat from {service_name}")
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON received from {address}")
                    
        except Exception as e:
            logger.error(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()
    
    def _check_heartbeats(self):
        while True:
            current_time = time.time() * 1000  # timestamp en milisegundos
            services_to_restart = set()
            
            with self.lock:
                for service_name, last_heartbeat in list(self.services.items()):
                    if current_time - last_heartbeat > self.heartbeat_timeout:
                        logger.warning(f"Service {service_name} has not sent heartbeat for {self.heartbeat_timeout}ms")
                        services_to_restart.add(service_name)
                        del self.services[service_name]
            
            # Intentar reiniciar los servicios caídos
            for service_name in services_to_restart:
                self._restart_service(service_name)
            
            time.sleep(self.heartbeat_interval / 1000)  # convertir a segundos
    
    def _restart_service(self, service_name: str):
        try:
            containers = self.docker_client.containers.list(all=True)
            # Buscar coincidencia exacta al final del nombre del contenedor
            matching = [
                c for c in containers
                if c.name.endswith(f"-{service_name}-1") or c.name.endswith(f"_{service_name}-1")
            ]
            if not matching:
                # Si no hay coincidencia exacta, buscar por substring (fallback)
                matching = [c for c in containers if service_name in c.name]
            if matching:
                container = matching[0]
                logger.info(f"Restarting service {service_name} (container: {container.name})")
                container.restart()
                logger.info(f"Service {service_name} restarted successfully")
            else:
                logger.error(f"Container for service {service_name} not found (searched for: {service_name})")
        except Exception as e:
            logger.error(f"Error restarting service {service_name}: {e}")

if __name__ == "__main__":
    monitor = ServiceMonitor()
    monitor.start()

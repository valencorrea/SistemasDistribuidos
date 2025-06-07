# Monitor Cluster con Bully y monitoreo de monitores
import socket
import threading
import time
import json
import os
import docker
import logging
import random
from typing import Dict, List, Tuple
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MonitorState(Enum):
    FOLLOWER = "follower"
    LEADER = "leader"

class MonitorCluster:
    def __init__(self):
        self.node_id = int(os.getenv('MONITOR_NODE_ID', str(random.randint(1000, 9999))))
        self.port = int(os.getenv('MONITOR_SERVICE_PORT', 50000 + self.node_id))
        self.cluster_port = int(os.getenv('MONITOR_CLUSTER_PORT', 50010 + self.node_id))
        self.heartbeat_interval = int(os.getenv('HEARTBEAT_INTERVAL', 5000))
        self.heartbeat_timeout = int(os.getenv('HEARTBEAT_TIMEOUT', 15000))

        self.state = MonitorState.FOLLOWER
        self.current_leader = None
        self.last_leader_heartbeat = 0

        self.cluster_nodes = self._get_cluster_nodes()
        self.expected_node_ids = [int(host.split("_")[1]) for host, _ in self.cluster_nodes]

        self.total_nodes = len(self.cluster_nodes) + 1

        self.services: Dict[str, float] = {}
        self.monitor_heartbeats: Dict[int, float] = {}
        self.lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.docker_client = docker.from_env()

        logger.info(f"Monitor {self.node_id} iniciado en puerto {self.port}, cluster en puerto {self.cluster_port}")
        logger.info(f"Nodos del cluster: {self.cluster_nodes}, total nodos: {self.total_nodes}")

    def _get_cluster_nodes(self) -> List[Tuple[str, int]]:
        nodes_env = os.getenv('MONITOR_CLUSTER_NODES', 'monitor_1,monitor_2,monitor_3')
        nodes = [node.strip() for node in nodes_env.split(',')]
        endpoints = []
        for node in nodes:
            try:
                node_id = int(node.split('_')[1])
                port = 50010 + node_id
                if node_id != self.node_id:
                    endpoints.append((node, port))
            except (IndexError, ValueError):
                logger.warning(f"Formato de nodo inválido: {node}")
        return endpoints

    def start(self):
        threading.Thread(target=self._start_service_server, daemon=True).start()
        threading.Thread(target=self._start_cluster_server, daemon=True).start()
        threading.Thread(target=self._check_service_heartbeats, daemon=True).start()
        threading.Thread(target=self._check_monitor_heartbeats, daemon=True).start()
        threading.Thread(target=self._election_loop, daemon=True).start()
        threading.Thread(target=self._leader_heartbeat_loop, daemon=True).start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info(f"Monitor {self.node_id} apagándose...")

    def _start_service_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('0.0.0.0', self.port))
            sock.listen(5)
            logger.info(f"Monitor {self.node_id} escuchando servicios en puerto {self.port}")
            while True:
                client, _ = sock.accept()
                with self.state_lock:
                    if self.state == MonitorState.LEADER:
                        threading.Thread(target=self._handle_service_client, args=(client,), daemon=True).start()
                    else:
                        client.close()
        except Exception as e:
            logger.error(f"Error servicio server: {e}")
        finally:
            sock.close()

    def _start_cluster_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('0.0.0.0', self.cluster_port))
            sock.listen(5)
            logger.info(f"Monitor {self.node_id} escuchando cluster en puerto {self.cluster_port}")
            while True:
                client, _ = sock.accept()
                threading.Thread(target=self._handle_cluster_client, args=(client,), daemon=True).start()
        except Exception as e:
            logger.error(f"Error cluster server: {e}")
        finally:
            sock.close()

    def _handle_cluster_client(self, client: socket.socket):
        try:
            data = client.recv(1024)
            if not data:
                return
            msg = json.loads(data.decode())
            if msg.get("type") == "bully_ping":
                sender_id = msg.get("sender_id")
                if sender_id < self.node_id:
                    client.send(json.dumps({"type": "bully_ok"}).encode())
            elif msg.get("type") == "who_is_leader":
                response = {"type": "leader_info", "leader_id": self.current_leader}
                client.send(json.dumps(response).encode())
            elif msg.get("type") == "leader_heartbeat":
                leader_id = msg.get("leader_id")
                with self.state_lock:
                    self.current_leader = leader_id
                    self.last_leader_heartbeat = time.time() * 1000
            elif msg.get("type") == "monitor_heartbeat":
                sender_id = msg.get("node_id")
                if sender_id is not None:
                    with self.lock:
                        self.monitor_heartbeats[sender_id] = time.time() * 1000
        except Exception as e:
            logger.error(f"Error cluster client: {e}")
        finally:
            client.close()

    def _handle_service_client(self, client: socket.socket):
        try:
            while True:
                data = client.recv(1024)
                if not data:
                    break
                try:
                    msg = json.loads(data.decode())
                    service = msg.get("service_name")
                    if service:
                        with self.lock:
                            self.services[service] = time.time() * 1000
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            logger.error(f"Error servicio client: {e}")
        finally:
            client.close()

    def _election_loop(self):
        time.sleep(random.uniform(1, 3))
        while True:
            with self.state_lock:
                now = time.time() * 1000
                if self.state != MonitorState.LEADER:
                    leader_alive = (
                        self.current_leader is not None and
                        (now - self.last_leader_heartbeat) < self.heartbeat_timeout
                    )
                    if not leader_alive:
                        logger.info(f"Monitor {self.node_id} no detecta líder activo. Inicia Bully.")
                        self.current_leader = None
                        self._start_bully_election()
            if self.state != MonitorState.LEADER:
                self._send_heartbeat_to_leader()
            time.sleep(5)

    def _send_heartbeat_to_leader(self):
        if self.current_leader is None:
            return
        try:
            leader_host = f"monitor_{self.current_leader}"
            leader_port = 50010 + self.current_leader
            sock = socket.socket()
            sock.settimeout(1)
            sock.connect((leader_host, leader_port))
            msg = json.dumps({"type": "monitor_heartbeat", "node_id": self.node_id})
            sock.send(msg.encode())
            sock.close()
        except Exception as e:
            logger.warning(f"No se pudo enviar heartbeat a líder {self.current_leader}: {e}")

    def _start_bully_election(self):
        higher_nodes = [(h, p) for h, p in self.cluster_nodes if int(h.split('_')[1]) > self.node_id]
        responded = False
        for host, port in higher_nodes:
            try:
                s = socket.socket()
                s.settimeout(1)
                s.connect((host, port))
                msg = {"type": "bully_ping", "sender_id": self.node_id}
                s.send(json.dumps(msg).encode())
                if s.recv(1024):
                    responded = True
                s.close()
            except:
                continue
        if not responded:
            self._become_leader()

    def _become_leader(self):
        self.state = MonitorState.LEADER
        self.current_leader = self.node_id
        logger.info(f"Monitor {self.node_id} ahora es el LIDER")
        self._send_heartbeat_to_all()

    def _send_heartbeat_to_all(self):
        msg = json.dumps({"type": "leader_heartbeat", "leader_id": self.node_id}).encode()
        for host, port in self.cluster_nodes:
            try:
                s = socket.socket()
                s.settimeout(1)
                s.connect((host, port))
                s.send(msg)
                s.close()
            except:
                continue

    def _leader_heartbeat_loop(self):
        while True:
            with self.state_lock:
                if self.state == MonitorState.LEADER:
                    self._send_heartbeat_to_all()
            time.sleep(self.heartbeat_interval / 1000)

    def _check_service_heartbeats(self):
        while True:
            try:
                with self.state_lock:
                    if self.state == MonitorState.LEADER:
                        now = time.time() * 1000
                        restart = []
                        with self.lock:
                            for name, ts in list(self.services.items()):
                                if now - ts > self.heartbeat_timeout:
                                    logger.warning(f"El servicio {name} no responde, se intentará reiniciar.")
                                    restart.append(name)
                                    del self.services[name]
                        for name in restart:
                            self._restart_service(name)
                time.sleep(self.heartbeat_interval / 1000)
            except Exception as e:
                logger.error(f"Error heartbeat check: {e}")

    def _check_monitor_heartbeats(self):
        while True:
            try:
                with self.state_lock:
                    if self.state == MonitorState.LEADER:
                        now = time.time() * 1000
                        restart = []
                        with self.lock:
                            # Iterar sobre todos los nodos esperados del cluster
                            for node_id in self.expected_node_ids:
                                if node_id == self.node_id:
                                    continue  # No me autoreinicio

                                last_beat = self.monitor_heartbeats.get(node_id)
                                if last_beat is None or (now - last_beat) > self.heartbeat_timeout:
                                    logger.warning(f"Monitor {node_id} no responde, se intentará reiniciar.")
                                    restart.append(node_id)
                                    self.monitor_heartbeats.pop(node_id, None)

                        for node_id in restart:
                            self._restart_monitor_container(node_id)

                time.sleep(self.heartbeat_interval / 1000)
            except Exception as e:
                logger.error(f"Error monitor heartbeat check: {e}")


    def _restart_service(self, name: str):
        try:
            containers = self.docker_client.containers.list(all=True)
            matching = [c for c in containers if name in c.name]
            if matching:
                container = matching[0]
                container.restart()
                logger.info(f"Servicio {name} reiniciado correctamente (contenedor: {container.name})")
            else:
                logger.error(f"Contenedor para {name} no encontrado")
        except Exception as e:
            logger.error(f"Error reiniciando {name}: {e}")

    def _restart_monitor_container(self, node_id: int):
        try:
            containers = self.docker_client.containers.list(all=True)
            target_name = f"monitor_{node_id}"
            matching = [c for c in containers if target_name in c.name]
            if matching:
                container = matching[0]
                container.restart()
                logger.info(f"Monitor {node_id} reiniciado correctamente (contenedor: {container.name})")
            else:
                logger.error(f"Contenedor para monitor_{node_id} no encontrado")
        except Exception as e:
            logger.error(f"Error reiniciando monitor_{node_id}: {e}")

if __name__ == "__main__":
    MonitorCluster().start()

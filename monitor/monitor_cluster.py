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
    CANDIDATE = "candidate"

class MonitorCluster:
    def __init__(self):
        self.node_id = int(os.getenv('MONITOR_NODE_ID', str(random.randint(1000, 9999))))
        self.port = int(os.getenv('MONITOR_SERVICE_PORT', 50000 + self.node_id))
        self.cluster_port = int(os.getenv('MONITOR_CLUSTER_PORT', 50010 + self.node_id))
        self.heartbeat_interval = int(os.getenv('HEARTBEAT_INTERVAL', 100))
        self.heartbeat_timeout = int(os.getenv('HEARTBEAT_TIMEOUT', 3000))
        self.election_timeout = int(os.getenv('ELECTION_TIMEOUT', 1000))

        self.state = MonitorState.FOLLOWER
        self.current_leader = None
        self.last_leader_heartbeat = 0
        self.election_term = 0 
        self.election_in_progress = False
        self.last_heartbeat_sent = 0

        self.cluster_nodes = self._get_cluster_nodes()
        self.expected_node_ids = [int(host.split("_")[1]) for host, _ in self.cluster_nodes]

        self.total_nodes = len(self.cluster_nodes) + 1

        self.expected_services = self._get_expected_services()
        self.services: Dict[str, float] = {}
        self.monitor_heartbeats: Dict[int, float] = {}
        self.service_start_times: Dict[str, float] = {}
        self.lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.docker_client = docker.from_env()
        self.leader_start_time = time.time() * 1000

        logger.info(f"🚀 Monitor {self.node_id} iniciado en puerto {self.port}, cluster en puerto {self.cluster_port}")
        logger.info(f"📋 Nodos del cluster: {self.cluster_nodes}, total nodos: {self.total_nodes}")
        logger.info(f"🔧 Servicios esperados: {self.expected_services}")

    def _get_expected_services(self) -> List[str]:
        services_env = os.getenv('EXPECTED_SERVICES', '')
        if services_env:
            services = [service.strip() for service in services_env.split(',') if service.strip()]
            filtered_services = self._filter_services(services)
            logger.info(f"📋 Servicios configurados desde docker-compose: {services}")
            logger.info(f"🔧 Servicios filtrados para monitoreo: {filtered_services}")
            return filtered_services
        else:
            default_services = [
                    'arg_production_filter', 'best_and_worst_ratings_aggregator', 
                    'credits_joiner', 'esp_production_filter', 'main_movie_filter', 
                    'no_colab_productions_filter', 'ratings_joiner', 
                    'sentiment_aggregator', 'sentiment_filter', 
                    'aggregator_top_10', 'top_5_countries_aggregator',
                    'twentieth_century_arg_aggregator', 'twentieth_century_arg_esp_aggregator',
                    'twentieth_century_filter'
                ]
            return default_services

    def _filter_services(self, services: List[str]) -> List[str]:
        excluded_services = {'worker', 'client_decodifier'}
        
        client_services = [service for service in services if service.startswith('client_')]
        excluded_services.update(client_services)
        
        filtered_services = [service for service in services if service not in excluded_services]
        
        logger.info(f"🚫 Servicios excluidos del monitoreo: {list(excluded_services)}")
        
        return filtered_services


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
        self._discover_existing_leader()
        
        threading.Thread(target=self._start_service_server, daemon=True).start()
        threading.Thread(target=self._start_cluster_server, daemon=True).start()
        threading.Thread(target=self._check_service_heartbeats, daemon=True).start()
        threading.Thread(target=self._check_monitor_heartbeats, daemon=True).start()
        threading.Thread(target=self._election_loop, daemon=True).start()
        threading.Thread(target=self._leader_heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._follower_heartbeat_loop, daemon=True).start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info(f"Monitor {self.node_id} apagándose...")

    def _discover_existing_leader(self):
        logger.info(f"🔍 Monitor {self.node_id} buscando líder existente...")
        
        time.sleep(3)
        
        for host, port in self.cluster_nodes:
            try:
                logger.info(f"🔍 Monitor {self.node_id} preguntando a {host}:{port} quién es el líder")
                s = socket.socket()
                s.settimeout(2)
                s.connect((host, port))
                
                msg = {"type": "who_is_leader"}
                s.send(json.dumps(msg).encode())
                
                response = s.recv(1024)
                if response:
                    response_data = json.loads(response.decode())
                    leader_id = response_data.get("leader_id")
                    
                    logger.info(f"🔍 Monitor {self.node_id} recibió respuesta de {host}: líder = {leader_id}")
                    
                    if leader_id is not None:
                        logger.info(f"✅ Monitor {self.node_id} descubrió líder existente: {leader_id}")
                        with self.state_lock:
                            self.current_leader = leader_id
                            self.last_leader_heartbeat = time.time() * 1000
                            self.election_term = max(self.election_term, 1)
                        s.close()
                        return
                s.close()
            except Exception as e:
                logger.debug(f"❌ No se pudo conectar con {host} para descubrir líder: {e}")
                continue
        
        logger.info(f"❌ Monitor {self.node_id} no encontró líder existente")

    def _start_service_server(self):
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind(('0.0.0.0', self.port))
        tcp_sock.listen(20)
        logger.info(f"🔧 Monitor {self.node_id} escuchando servicios TCP en puerto {self.port}")

        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_sock.bind(('0.0.0.0', self.port))
        logger.info(f"🔧 Monitor {self.node_id} escuchando servicios UDP en puerto {self.port}")

        def tcp_loop():
            while True:
                client, addr = tcp_sock.accept()
                threading.Thread(target=self._handle_service_client, args=(client,), daemon=True).start()

        def udp_loop():
            while True:
                try:
                    data, addr = udp_sock.recvfrom(1024)
                    threading.Thread(target=self._handle_service_udp, args=(data, addr), daemon=True).start()
                except Exception as e:
                    logger.error(f"Error recibiendo UDP: {e}")

        threading.Thread(target=tcp_loop, daemon=True).start()
        threading.Thread(target=udp_loop, daemon=True).start()

    def _start_cluster_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('0.0.0.0', self.cluster_port))
            sock.listen(5)
            logger.info(f"🌐 Monitor {self.node_id} escuchando cluster en puerto {self.cluster_port}")
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
                logger.info(f"📨 Monitor {self.node_id} recibió bully_ping de {sender_id}")
                
                with self.state_lock:
                    if self.state == MonitorState.LEADER:
                        response = {"type": "already_leader", "leader_id": self.node_id, "term": self.election_term}
                        client.send(json.dumps(response).encode())
                        logger.info(f"👑 Monitor {self.node_id} respondió que ya es líder a {sender_id}")
                        return
                
                if sender_id < self.node_id:
                    client.send(json.dumps({"type": "bully_ok"}).encode())
                    logger.info(f"✅ Monitor {self.node_id} respondió bully_ok a {sender_id}")
                    
                    with self.state_lock:
                        if self.state != MonitorState.LEADER and not self.election_in_progress:
                            logger.info(f"🎯 Monitor {self.node_id} inicia elección propia después de recibir bully_ping")
                            threading.Thread(target=self._start_bully_election, daemon=True).start()
                            
            elif msg.get("type") == "bully_ok":
                logger.info(f"📨 Monitor {self.node_id} recibió bully_ok de nodo superior")
                
            elif msg.get("type") == "already_leader":
                leader_id = msg.get("leader_id")
                term = msg.get("term", 0)
                logger.info(f"👑 Monitor {self.node_id} recibió respuesta de que {leader_id} ya es líder (término {term})")
                
                with self.state_lock:
                    if term >= self.election_term:
                        self.current_leader = leader_id
                        self.election_term = term
                        self.state = MonitorState.FOLLOWER
                        self.last_leader_heartbeat = time.time() * 1000
                        logger.info(f"✅ Monitor {self.node_id} reconoce a {leader_id} como líder existente")
                
            elif msg.get("type") == "leader_announcement":
                leader_id = msg.get("leader_id")
                term = msg.get("term", 0)
                logger.info(f"📢 Monitor {self.node_id} recibió anuncio de líder {leader_id} (término {term})")
                
                with self.state_lock:
                    if term >= self.election_term:
                        self.current_leader = leader_id
                        self.election_term = term
                        self.state = MonitorState.FOLLOWER
                        self.last_leader_heartbeat = time.time() * 1000
                        logger.info(f"✅ Monitor {self.node_id} reconoce a {leader_id} como líder")
                
            elif msg.get("type") == "who_is_leader":
                response = {"type": "leader_info", "leader_id": self.current_leader}
                client.send(json.dumps(response).encode())
                logger.info(f"❓ Monitor {self.node_id} respondió who_is_leader: {self.current_leader}")
                
            elif msg.get("type") == "leader_heartbeat":
                leader_id = msg.get("leader_id")
                term = msg.get("term", 0)
                
                with self.state_lock:
                    if term >= self.election_term:
                        self.current_leader = leader_id
                        self.election_term = term
                        self.last_leader_heartbeat = time.time() * 1000
                        
                        if self.state == MonitorState.LEADER and leader_id != self.node_id:
                            logger.info(f"👑 Monitor {self.node_id} recibió heartbeat de líder {leader_id}. Dejando de ser líder.")
                            self.state = MonitorState.FOLLOWER
                        
                        logger.debug(f"💓 Monitor {self.node_id} recibió heartbeat de líder {leader_id} (término {term})")

            elif msg.get("type") == "monitor_heartbeat":
                sender_id = msg.get("node_id")
                if sender_id is not None:
                    with self.lock:
                        self.monitor_heartbeats[sender_id] = (time.time() * 1000) + self.heartbeat_timeout
                        
        except Exception as e:
            logger.error(f"Error cluster client: {e}")
        finally:
            client.close()

    def _handle_service_client(self, client: socket.socket):
        try:
            buffer = ""
            while True:
                data = client.recv(1024)
                if not data:
                    break
                
                buffer += data.decode()
                
                while True:
                    try:
                        brace_count = 0
                        json_end = -1
                        
                        for i, char in enumerate(buffer):
                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    json_end = i + 1
                                    break
                        
                        if json_end == -1:
                            break
                        
                        json_str = buffer[:json_end]
                        buffer = buffer[json_end:]
                        
                        msg = json.loads(json_str)
                        
                        if msg.get("type") == "who_is_leader":
                            response = {"type": "leader_info", "leader_id": self.current_leader}
                            client.send(json.dumps(response).encode())
                            continue
                        
                        service = msg.get("service_name")
                        if service:
                            with self.lock:
                                if service not in self.services and service in self.service_start_times:
                                    del self.service_start_times[service]
                                
                                self.services[service] = (time.time() * 1000 )+ self.heartbeat_timeout
                        else:
                            logger.warning(f"⚠️ Monitor {self.node_id} recibió mensaje sin service_name: {msg}")
                            
                    except json.JSONDecodeError as e:
                        next_brace = buffer.find('{')
                        if next_brace == -1:
                            buffer = ""
                            break
                        else:
                            buffer = buffer[next_brace:]
                            continue
                            
        except Exception as e:
            logger.error(f"Error servicio client: {e}")
        finally:
            client.close()

    def _handle_service_udp(self, data, addr):
        try:
            msg = json.loads(data.decode())
            service = msg.get("service_name")
            if service:
                with self.lock:
                    if service not in self.services and service in self.service_start_times:
                        del self.service_start_times[service]
                    self.services[service] = (time.time() * 1000 )+ self.heartbeat_timeout
            else:
                logger.warning(f"⚠️ Monitor {self.node_id} recibió mensaje UDP sin service_name: {msg}")
        except Exception as e:
            logger.error(f"Error recibiendo UDP: {e}")

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
                    if not leader_alive and not self.election_in_progress:
                        #logger.info(f"⏰ Monitor {self.node_id} no detecta líder activo. Verificando antes de iniciar elección...")
                        
                        if self._quick_leader_check():
                            logger.info(f"✅ Monitor {self.node_id} encontró líder en verificación rápida, no inicia elección")
                        else:
                            #logger.info(f"⏰ Monitor {self.node_id} confirma que no hay líder activo. Inicia elección Bully.")
                            self.current_leader = None
                            threading.Thread(target=self._start_bully_election, daemon=True).start()
            time.sleep(1)

    def _quick_leader_check(self):
        for host, port in self.cluster_nodes:
            try:
                s = socket.socket()
                s.settimeout(1)
                s.connect((host, port))
                
                msg = {"type": "who_is_leader"}
                s.send(json.dumps(msg).encode())
                
                response = s.recv(1024)
                if response:
                    response_data = json.loads(response.decode())
                    leader_id = response_data.get("leader_id")
                    
                    if leader_id is not None:
                        logger.info(f"🔍 Monitor {self.node_id} encontró líder {leader_id} en verificación rápida")
                        with self.state_lock:
                            self.current_leader = leader_id
                            self.last_leader_heartbeat = time.time() * 1000
                        s.close()
                        return True
                s.close()
            except Exception as e:
                logger.debug(f"❌ No se pudo verificar líder en {host}: {e}")
                continue
        
        return False

    def _follower_heartbeat_loop(self):
        while True:
            if self.state != MonitorState.LEADER and self.current_leader is not None:
                self._send_heartbeat_to_leader()
            time.sleep(self.heartbeat_interval / 1000)

    def _send_heartbeat_to_leader(self):
        if self.current_leader is None:
            return
            
        try:
            leader_host = None
            leader_port = None
            
            for host, port in self.cluster_nodes:
                if int(host.split('_')[1]) == self.current_leader:
                    leader_host = host
                    leader_port = port
                    break
            
            if leader_host is None:
                leader_host = f"monitor_{self.current_leader}"
                leader_port = 50010 + self.current_leader
            
            sock = socket.socket()
            sock.settimeout(2)
            sock.connect((leader_host, leader_port))
            msg = json.dumps({"type": "monitor_heartbeat", "node_id": self.node_id})
            sock.send(msg.encode())
            sock.close()
            
            #logger.debug(f"💓 Monitor {self.node_id} envió heartbeat a líder {self.current_leader}")
        except Exception as e:
            #logger.warning(f"❌ No se pudo enviar heartbeat a líder {self.current_leader}: {e}")
            logger.info(f"🚨 Monitor {self.node_id} detectó que el líder {self.current_leader} no está disponible. Iniciando elección.")
            with self.state_lock:
                self.current_leader = None
                self.last_leader_heartbeat = 0
            threading.Thread(target=self._start_bully_election, daemon=True).start()

    def _start_bully_election(self):
        with self.state_lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            self.state = MonitorState.CANDIDATE
            self.election_term += 1
            current_term = self.election_term
            
        logger.info(f"🎯 Monitor {self.node_id} inicia elección Bully (término {current_term})")
        
        try:
            higher_nodes = [(h, p) for h, p in self.cluster_nodes if int(h.split('_')[1]) > self.node_id]
            logger.info(f"🎯 Monitor {self.node_id} enviará bully_ping a nodos superiores: {higher_nodes}")
            
            responded = False
            found_existing_leader = False
            
            for host, port in higher_nodes:
                try:
                    logger.info(f"📨 Monitor {self.node_id} enviando bully_ping a {host}:{port}")
                    s = socket.socket()
                    s.settimeout(2)
                    s.connect((host, port))
                    msg = {"type": "bully_ping", "sender_id": self.node_id}
                    s.send(json.dumps(msg).encode())
                    
                    response = s.recv(1024)
                    if response:
                        response_data = json.loads(response.decode())
                        logger.info(f"📨 Monitor {self.node_id} recibió respuesta de {host}: {response_data}")
                        
                        if response_data.get("type") == "already_leader":
                            leader_id = response_data.get("leader_id")
                            term = response_data.get("term", 0)
                            logger.info(f"👑 Monitor {self.node_id} encontró líder existente {leader_id} (término {term})")
                            
                            with self.state_lock:
                                if term >= self.election_term:
                                    self.current_leader = leader_id
                                    self.election_term = term
                                    self.state = MonitorState.FOLLOWER
                                    self.last_leader_heartbeat = time.time() * 1000
                                    logger.info(f"✅ Monitor {self.node_id} reconoce a {leader_id} como líder existente")
                                    found_existing_leader = True
                                    break
                        else:
                            responded = True
                            logger.info(f"✅ Monitor {self.node_id} recibió respuesta de {host}")
                    s.close()
                except Exception as e:
                    logger.debug(f"❌ No se pudo conectar con {host}: {e}")
                    continue
            
            if found_existing_leader:
                logger.info(f"✅ Monitor {self.node_id} encontró líder existente, terminando elección")
                return
            
            if not responded:
                logger.info(f"👑 Monitor {self.node_id} no recibió respuestas de nodos superiores. Se convierte en líder.")
                self._become_leader(current_term)
            else:
                logger.info(f"⏳ Monitor {self.node_id} recibió respuestas de nodos superiores. Espera elección.")
                time.sleep(10)
                
        finally:
            with self.state_lock:
                self.election_in_progress = False
                if self.state == MonitorState.CANDIDATE:
                    self.state = MonitorState.FOLLOWER

    def _become_leader(self, term):
        with self.state_lock:
            self.state = MonitorState.LEADER
            self.current_leader = self.node_id
            self.election_term = term
            
        logger.info(f"👑 Monitor {self.node_id} ahora es el LÍDER (término {term})")
        
        self._initialize_expected_services()
        self._initialize_expected_monitors()
        
        self._announce_leadership()
        self._send_heartbeat_to_all()

    def _initialize_expected_services(self):
        logger.info(f"🔧 Monitor {self.node_id} inicializando tracking de {len(self.expected_services)} servicios esperados")
        
        now = time.time() * 1000
        with self.lock:
            for service_name in self.expected_services:
                if service_name not in self.services:
                    self.service_start_times[service_name] = now
                    logger.info(f"⏳ Monitor {self.node_id} esperando primer heartbeat de {service_name}")

    def _initialize_expected_monitors(self):
        logger.info(f"🔧 Monitor {self.node_id} inicializando tracking de {len(self.expected_node_ids)} monitores esperados")
        
        now = time.time() * 1000
        with self.lock:
            for node_id in self.expected_node_ids:
                if node_id != self.node_id and node_id not in self.monitor_heartbeats:
                    self.monitor_heartbeats[node_id] = now + 20000
                    logger.info(f"⏳ Monitor {self.node_id} esperando primer heartbeat de monitor {node_id}")

    def _announce_leadership(self):
        msg = json.dumps({
            "type": "leader_announcement", 
            "leader_id": self.node_id,
            "term": self.election_term
        }).encode()
        
        for host, port in self.cluster_nodes:
            try:
                s = socket.socket()
                s.settimeout(2)
                s.connect((host, port))
                s.send(msg)
                s.close()
                logger.info(f"📢 Monitor {self.node_id} anunció liderazgo a {host}")
            except Exception as e:
                logger.warning(f"❌ No se pudo anunciar liderazgo a {host}: {e}")

    def _send_heartbeat_to_all(self):
        msg = json.dumps({
            "type": "leader_heartbeat", 
            "leader_id": self.node_id,
            "term": self.election_term
        }).encode()
        
        for host, port in self.cluster_nodes:
            try:
                s = socket.socket()
                s.settimeout(1)
                s.connect((host, port))
                s.send(msg)
                s.close()
            except Exception as e:
                logger.debug(f"❌ No se pudo enviar heartbeat a {host}: {e}")

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
                                    restart.append(name)
                                    del self.services[name]
                            
                            initial_timeout = 20000
                            for service_name in self.expected_services:
                                if service_name not in self.services:
                                    start_time = self.service_start_times.get(service_name)
                                    if start_time and (now - start_time) > initial_timeout:
                                        restart.append(service_name)
                                        del self.service_start_times[service_name]
                                        logger.warning(f"⚠️ Servicio {service_name} nunca ha reportado heartbeat después de {initial_timeout/1000}s, se intentará levantar")
                        
                        for name in restart:
                            self._restart_service(name)
                            
                time.sleep(self.heartbeat_interval / 1000)
            except Exception as e:
                logger.error(f"Error heartbeat check: {e}")

    def _restart_service(self, name: str):
        try:
            containers = self.docker_client.containers.list(all=True)
            
            matching = []
            
            for container in containers:
                container_name = container.name
                
                if name in container_name:
                    if (container_name == name or 
                        container_name.endswith(f"_{name}") or
                        container_name.endswith(f"-{name}") or
                        any(container_name.endswith(f"{name}_{i}") for i in range(1, 100)) or
                        any(container_name.endswith(f"{name}-{i}") for i in range(1, 100)) or
                        any(container_name.endswith(f"{name}_{i}-{j}") for i in range(1, 100) for j in range(1, 100))):
                        matching.append(container)
            
            if not matching:
                logger.warning(f"❌ Contenedor para {name} no encontrado")
                return
            
            logger.info(f"🔍 Encontrados {len(matching)} contenedores para {name}: {[c.name for c in matching]}")
            
            for container in matching:
                try:
                    logger.info(f"🔄 Reiniciando contenedor {container.name} (sin verificar status)")
                    container.restart()
                    logger.info(f"✅ Contenedor {container.name} reiniciado correctamente")
                except Exception as e:
                    logger.error(f"❌ Error reiniciando {container.name}: {e}")
            
            with self.lock:
                now = time.time() * 1000
                additional_delay = 60000 if "sentiment" in name.lower() else 30000
                self.services[name] = now + additional_delay
                
        except Exception as e:
            logger.error(f"❌ Error reiniciando {name}: {e}")

    def _check_monitor_heartbeats(self):
        while True:
            try:
                with self.state_lock:
                    if self.state == MonitorState.LEADER:
                        now = time.time() * 1000
                        restart = []
                        with self.lock:
                            for node_id in self.expected_node_ids:
                                if node_id == self.node_id:
                                    continue

                                last_beat = self.monitor_heartbeats.get(node_id)
                                if last_beat is None:
                                    self.monitor_heartbeats[node_id] = now + 10000
                                    logger.info(f"⏳ Monitor {self.node_id} inicializando tracking de monitor {node_id}")
                                elif (now - last_beat) > self.heartbeat_timeout:
                                    if (now - last_beat) < 60000:
                                        logger.warning(f"⚠️ Monitor {node_id} no responde, se intentará reiniciar.")
                                        restart.append(node_id)
                                        self.monitor_heartbeats.pop(node_id, None)

                        for node_id in restart:
                            self._restart_monitor_container(node_id)

                time.sleep(self.heartbeat_interval / 1000)
            except Exception as e:
                logger.error(f"Error monitor heartbeat check: {e}")

    def _restart_monitor_container(self, node_id: int):
        try:
            containers = self.docker_client.containers.list(all=True)
            target_name = f"monitor_{node_id}"
            
            matching = []
            
            for container in containers:
                container_name = container.name
                
                if target_name in container_name:
                    if (container_name == target_name or 
                        container_name.endswith(f"_{target_name}") or
                        container_name.endswith(f"-{target_name}") or
                        any(container_name.endswith(f"{target_name}_{i}") for i in range(1, 100)) or
                        any(container_name.endswith(f"{target_name}-{i}") for i in range(1, 100)) or
                        any(container_name.endswith(f"{target_name}_{i}-{j}") for i in range(1, 100) for j in range(1, 100))):
                        matching.append(container)
            
            if not matching:
                logger.warning(f"❌ Contenedor para monitor_{node_id} no encontrado")
                return
            
            logger.info(f"🔍 Encontrados {len(matching)} contenedores para monitor_{node_id}: {[c.name for c in matching]}")
            
            for container in matching:
                try:
                    logger.info(f"🔄 Reiniciando contenedor {container.name} (sin verificar status)")
                    container.restart()
                    logger.info(f"✅ Contenedor {container.name} reiniciado correctamente")
                except Exception as e:
                    logger.error(f"❌ Error reiniciando {container.name}: {e}")
            
            with self.lock:
                self.monitor_heartbeats[node_id] = time.time() * 1000 + 15000
            logger.info(f"✅ Monitor {node_id} marcado como saludable después de intentar reiniciar")
                
        except Exception as e:
            logger.error(f"❌ Error reiniciando monitor_{node_id}: {e}")

if __name__ == "__main__":
    MonitorCluster().start()

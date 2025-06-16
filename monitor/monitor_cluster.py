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
        self.heartbeat_interval = int(os.getenv('HEARTBEAT_INTERVAL', 5000))
        self.heartbeat_timeout = int(os.getenv('HEARTBEAT_TIMEOUT', 15000))
        self.election_timeout = int(os.getenv('ELECTION_TIMEOUT', 10000))

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
        self.lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.docker_client = docker.from_env()

        logger.info(f"🚀 Monitor {self.node_id} iniciado en puerto {self.port}, cluster en puerto {self.cluster_port}")
        logger.info(f"📋 Nodos del cluster: {self.cluster_nodes}, total nodos: {self.total_nodes}")
        logger.info(f"🔧 Servicios esperados: {self.expected_services}")

    def _get_expected_services(self) -> List[str]:
        """Obtiene la lista de servicios que deben estar ejecutándose desde variables de entorno"""
        services_env = os.getenv('EXPECTED_SERVICES', '')
        if services_env:
            services = [service.strip() for service in services_env.split(',') if service.strip()]
            # Filtrar servicios que no deben ser monitoreados
            filtered_services = self._filter_services(services)
            logger.info(f"📋 Servicios configurados desde docker-compose: {services}")
            logger.info(f"🔧 Servicios filtrados para monitoreo: {filtered_services}")
            return filtered_services
        else:
            # Fallback: obtener servicios del docker-compose dinámicamente
            try:
                services = self._discover_services_from_compose()
                filtered_services = self._filter_services(services)
                logger.info(f"🔍 Servicios descubiertos dinámicamente: {services}")
                logger.info(f"🔧 Servicios filtrados para monitoreo: {filtered_services}")
                return filtered_services
            except Exception as e:
                logger.warning(f"❌ No se pudieron descubrir servicios dinámicamente: {e}")
                # Lista por defecto basada en el docker-compose (ya filtrada)
                default_services = [
                    'arg_production_filter', 'best_and_worst_ratings_aggregator', 
                    'credits_joiner', 'esp_production_filter', 'main_movie_filter', 
                    'no_colab_productions_filter', 'ratings_joiner', 
                    'sentiment_aggregator', 'sentiment_filter', 
                    'top_10_credits_aggregator', 'top_5_countries_aggregator',
                    'twentieth_century_arg_aggregator', 'twentieth_century_arg_esp_aggregator',
                    'twentieth_century_filter'
                ]
                logger.info(f"📋 Usando lista por defecto de servicios filtrados: {default_services}")
                return default_services

    def _filter_services(self, services: List[str]) -> List[str]:
        """Filtra la lista de servicios excluyendo worker y client_*"""
        excluded_services = {'worker', 'client_decodifier'}
        
        # Agregar todos los servicios que empiecen con 'client_'
        client_services = [service for service in services if service.startswith('client_')]
        excluded_services.update(client_services)
        
        # Filtrar servicios excluidos
        filtered_services = [service for service in services if service not in excluded_services]
        
        logger.info(f"🚫 Servicios excluidos del monitoreo: {list(excluded_services)}")
        
        return filtered_services

    def _discover_services_from_compose(self) -> List[str]:
        """Descubre servicios del docker-compose dinámicamente"""
        try:
            # Obtener todos los contenedores del proyecto
            containers = self.docker_client.containers.list(all=True)
            services = set()
            
            for container in containers:
                # Obtener labels del contenedor para identificar el servicio
                labels = container.labels
                service_name = labels.get('com.docker.compose.service')
                
                if service_name and service_name not in ['monitor_1', 'monitor_2', 'monitor_3', 'rabbitmq']:
                    services.add(service_name)
            
            return list(services)
        except Exception as e:
            logger.error(f"Error descubriendo servicios: {e}")
            return []

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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('0.0.0.0', self.port))
            sock.listen(5)
            logger.info(f"🔧 Monitor {self.node_id} escuchando servicios en puerto {self.port}")
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
                        self.monitor_heartbeats[sender_id] = time.time() * 1000
                        logger.debug(f"💓 Monitor {self.node_id} recibió heartbeat de monitor {sender_id}")
                        
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
                            logger.debug(f"💓 Monitor {self.node_id} recibió heartbeat de servicio {service}")
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
                    if not leader_alive and not self.election_in_progress:
                        logger.info(f"⏰ Monitor {self.node_id} no detecta líder activo. Verificando antes de iniciar elección...")
                        
                        if self._quick_leader_check():
                            logger.info(f"✅ Monitor {self.node_id} encontró líder en verificación rápida, no inicia elección")
                        else:
                            logger.info(f"⏰ Monitor {self.node_id} confirma que no hay líder activo. Inicia elección Bully.")
                            self.current_leader = None
                            threading.Thread(target=self._start_bully_election, daemon=True).start()
            time.sleep(5)

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
            
            logger.debug(f"💓 Monitor {self.node_id} envió heartbeat a líder {self.current_leader}")
        except Exception as e:
            logger.warning(f"❌ No se pudo enviar heartbeat a líder {self.current_leader}: {e}")
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
        
        self._announce_leadership()
        self._send_heartbeat_to_all()

    def _initialize_expected_services(self):
        """Inicializa el tracking de servicios esperados cuando se convierte en líder"""
        logger.info(f"🔧 Monitor {self.node_id} inicializando tracking de {len(self.expected_services)} servicios esperados")
        
        with self.lock:
            # Marcar todos los servicios esperados como no reportados inicialmente
            for service_name in self.expected_services:
                if service_name not in self.services:
                    # No establecer timestamp inicial, esperar primer heartbeat
                    logger.info(f"⏳ Monitor {self.node_id} esperando primer heartbeat de {service_name}")

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
                            # Verificar servicios que han reportado heartbeat
                            for name, ts in list(self.services.items()):
                                if now - ts > self.heartbeat_timeout:
                                    restart.append(name)
                                    del self.services[name]
                            
                            # Verificar servicios esperados que nunca han reportado
                            for service_name in self.expected_services:
                                if service_name not in self.services:
                                    # Si nunca ha reportado, verificar si existe el contenedor
                                    if self._should_restart_service(service_name):
                                        restart.append(service_name)
                                        logger.warning(f"⚠️ Servicio {service_name} nunca ha reportado heartbeat, se intentará levantar")
                        
                        for name in restart:
                            self._restart_service(name)
                            
                time.sleep(self.heartbeat_interval / 1000)
            except Exception as e:
                logger.error(f"Error heartbeat check: {e}")

    def _should_restart_service(self, service_name: str) -> bool:
        """Determina si un servicio debe ser reiniciado basado en su estado actual"""
        try:
            containers = self.docker_client.containers.list(all=True)
            matching = [c for c in containers if service_name in c.name]
            
            if not matching:
                logger.warning(f"❌ Contenedor para {service_name} no encontrado")
                return True
                
            container = matching[0]
            container.reload()
            status = container.status
            
            # Solo reiniciar si está detenido, muerto o pausado
            return status in ["exited", "dead", "paused"]
            
        except Exception as e:
            logger.error(f"❌ Error verificando estado de {service_name}: {e}")
            return False

    def _restart_service(self, name: str):
        try:
            containers = self.docker_client.containers.list(all=True)
            matching = [c for c in containers if name in c.name]
            
            if not matching:
                logger.warning(f"❌ Contenedor para {name} no encontrado")
                return
                
            container = matching[0]
            
            container.reload()
            status = container.status
            
            logger.info(f"🔍 Verificando estado de {name}: {status}")
            
            if status == "running":
                logger.info(f"✅ El servicio {name} está ejecutándose correctamente. No se reiniciará.")
                with self.lock:
                    self.services[name] = time.time() * 1000
                return
            elif status in ["exited", "dead", "paused"]:
                logger.warning(f"🔄 El servicio {name} está en estado {status}, se intentará reiniciar.")
                container.restart()
                logger.info(f"✅ Servicio {name} reiniciado correctamente (contenedor: {container.name})")
            else:
                logger.info(f"ℹ️ El servicio {name} está en estado {status}, no se reiniciará.")
                
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
                                if last_beat is None or (now - last_beat) > self.heartbeat_timeout:
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
            matching = [c for c in containers if target_name in c.name]
            
            if not matching:
                logger.warning(f"❌ Contenedor para monitor_{node_id} no encontrado")
                return
                
            container = matching[0]
            
            container.reload()
            status = container.status
            
            logger.info(f"🔍 Verificando estado de monitor_{node_id}: {status}")
            
            if status == "running":
                logger.info(f"✅ El monitor {node_id} está ejecutándose correctamente. No se reiniciará.")
                with self.lock:
                    self.monitor_heartbeats[node_id] = time.time() * 1000
                return
            elif status in ["exited", "dead", "paused"]:
                logger.warning(f"🔄 El monitor {node_id} está en estado {status}, se intentará reiniciar.")
                container.restart()
                logger.info(f"✅ Monitor {node_id} reiniciado correctamente (contenedor: {container.name})")
            else:
                logger.info(f"ℹ️ El monitor {node_id} está en estado {status}, no se reiniciará.")
                
        except Exception as e:
            logger.error(f"❌ Error reiniciando monitor_{node_id}: {e}")

if __name__ == "__main__":
    MonitorCluster().start()

import json
import logging
import os
import threading
from collections import defaultdict
import time
from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from utils.parsers.credits_parser import convert_data
from worker.worker import Worker
from middleware.tcp_protocol.tcp_protocol import TCPClient, TCPServer

from joiner.base.joiner_recovery_manager import JoinerRecoveryManager

PENDING_MESSAGES = "/root/files/credits_pending.jsonl"
BATCH_PERSISTENCE_DIR = "/root/files/credits_batches/"
CHECKPOINT_FILE = "/root/files/credits_checkpoint.json"
MOVIES_PERSISTENCE_FILE = "/root/files/credits_movies.json"  # Nuevo archivo para persistir movies

class CreditsJoinerSimple(Worker):
    """CreditsJoiner usando JoinerRecoveryManager"""
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%H:%M:%S')   
    def __init__(self):
        super().__init__()

        
        # Estado específico del credits joiner
        self.actor_counts = {}
        self.movies = {}
        self.processed_rating_batches_per_client = defaultdict(int)
        
        # ✅ AGREGAR: Variables para control de reintentos
        self._credits_consumer_started = False
        self._credits_consumer_lock = threading.Lock()
        self._consumer_retry_count = 0
        self._max_consumer_retries = 3
        self._consumer_retry_delay = 2  # segundos
        
        # Configurar recovery manager
        config = {
            'checkpoint_file': CHECKPOINT_FILE,
            'batch_persistence_dir': BATCH_PERSISTENCE_DIR,
            'aggregator_queue': 'credits_aggregator',
            'aggregator_response_queue': 'credits_joiner_response',
            'joiner_id': 'CreditsJoiner',
            'checkpoint_interval': 5,  
            'log_interval': 5,
            'aggregator_host': os.getenv("AGGREGATOR_HOST", "aggregator_top_10"),
            'aggregator_port': int(os.getenv("AGGREGATOR_PORT", 60000))
        }
        
        self.recovery_manager = JoinerRecoveryManager(config, self.logger)
        
        # Consumers y producers
        self.movies_consumer = Subscriber("20_century_arg_result",
                                        message_handler=self.handle_movies_message)
        self.credits_consumer = Consumer("credits",
                                        _message_handler=self.handle_credits_message)
        self.producer = Producer("top_10_actors_from_batch")
        self.credits_producer = Producer(
            queue_name="credits",
            queue_type="direct")
        self.joiner_instance_id = "joiner_credits"
        
        # Cargar estado inicial
        self._load_initial_state()
    
    def _load_initial_state(self):
        # Primero cargar movies desde persistencia
        self._load_movies_from_persistence()
        
        state_data = {
            'restore_state': self._restore_state,
            'save_state': self._save_state,
            'log_state': self._log_state,
            'process_message': self.process_credits_message
        }
        self.recovery_manager.load_checkpoint_and_recover(state_data)
    
    def _load_movies_from_persistence(self):
        """Carga movies desde archivo de persistencia"""
        if os.path.exists(MOVIES_PERSISTENCE_FILE):
            try:
                with open(MOVIES_PERSISTENCE_FILE, "r") as f:
                    movies_data = json.load(f)
                
                for client_id, client_data in movies_data["clients"].items():
                    self.movies[client_id] = set(client_data["movies"])
                
                self.logger.info(f"✅ Recuperadas movies de {len(self.movies)} clientes desde persistencia")
                
                # Log detallado de movies recuperadas
                for client_id, movies_set in self.movies.items():
                    self.logger.info(f"📽️ Cliente {client_id}: {len(movies_set)} películas recuperadas")
                    
            except Exception as e:
                self.logger.error(f"❌ Error cargando movies desde persistencia: {e}")
        else:
            self.logger.info("📽️ No se encontró archivo de persistencia de movies")
    
    def _persist_movies_immediately(self, client_id, movies):
        """Persiste movies inmediatamente para evitar pérdida"""
        try:
            # Cargar movies existentes o crear nuevo
            if os.path.exists(MOVIES_PERSISTENCE_FILE):
                with open(MOVIES_PERSISTENCE_FILE, "r") as f:
                    movies_data = json.load(f)
            else:
                movies_data = {"clients": {}}
            
            # Agregar/actualizar movies del cliente
            movies_data["clients"][client_id] = {
                "movies": list(movies),
                "timestamp": time.time(),
                "total_movies": len(movies)
            }
            
            # Guardar de forma atómica
            temp_file = MOVIES_PERSISTENCE_FILE + ".tmp"
            with open(temp_file, "w") as f:
                json.dump(movies_data, f, indent=2)
            
            os.replace(temp_file, MOVIES_PERSISTENCE_FILE)
            self.logger.info(f"✅ Movies del cliente {client_id} persistidas inmediatamente ({len(movies)} películas)")
            
        except Exception as e:
            self.logger.error(f"❌ Error persistiendo movies del cliente {client_id}: {e}")
    
    def _verify_movies_integrity(self, client_id, received_movies):
        """Verifica que las movies recibidas sean consistentes"""
        received_set = set(received_movies)
        
        if client_id in self.movies:
            existing_movies = self.movies[client_id]
            if existing_movies != received_set:
                self.logger.warning(f"⚠️ Inconsistencia en movies del cliente {client_id}")
                self.logger.warning(f"   Existentes: {len(existing_movies)}, Recibidas: {len(received_set)}")
                self.logger.warning(f"   Diferencia: {len(existing_movies.symmetric_difference(received_set))} películas")
                
                # Usar las más recientes (recibidas)
                self.logger.info(f"🔄 Usando movies más recientes para cliente {client_id}")
                return received_set
        
        return received_set
    
    def _restore_state(self, checkpoint_data):
        """Restaura estado específico del credits joiner"""
        self.actor_counts = checkpoint_data.get("actor_counts", {})
        self.processed_rating_batches_per_client = defaultdict(int, checkpoint_data.get("processed_rating_batches_per_client", {}))
        
        # Restaurar movies del checkpoint (convertir listas de vuelta a sets)
        checkpoint_movies = checkpoint_data.get("movies", {})
        
        # Verificar consistencia entre checkpoint y persistencia
        for client_id, movie_ids in checkpoint_movies.items():
            checkpoint_set = set(movie_ids)
            persistent_set = self.movies.get(client_id, set())
            
            if checkpoint_set != persistent_set:
                self.logger.warning(f"⚠️ Inconsistencia entre checkpoint y persistencia para cliente {client_id}")
                self.logger.warning(f"   Checkpoint: {len(checkpoint_set)}, Persistencia: {len(persistent_set)}")
                
                # Usar la fuente con más movies (más completa)
                if len(persistent_set) >= len(checkpoint_set):
                    self.logger.info(f"🔄 Usando movies de persistencia para cliente {client_id}")
                    self.movies[client_id] = persistent_set
                else:
                    self.logger.info(f"🔄 Usando movies de checkpoint para cliente {client_id}")
                    self.movies[client_id] = checkpoint_set
            else:
                self.movies[client_id] = checkpoint_set
        
        self.logger.info(f"✅ Estado restaurado: {len(self.actor_counts)} clientes con datos, {len(self.movies)} clientes con movies")
    
    def _save_state(self):
        """Guarda estado específico del credits joiner"""
        return {
            "actor_counts": dict(self.actor_counts),
            "processed_rating_batches_per_client": dict(self.processed_rating_batches_per_client),
            "movies": {client_id: list(movie_ids) for client_id, movie_ids in self.movies.items()}
        }
    
    def _log_state(self):
        """Log de estado específico del credits joiner"""
        self.logger.info(f"✅ Clientes con datos: {list(self.actor_counts.keys())}")
        self.logger.info(f"📽️ Clientes con movies: {list(self.movies.keys())}")
        
        for client_id, actor_data in self.actor_counts.items():
            top_5 = sorted(actor_data.items(), key=lambda item: item[1]["count"], reverse=True)[:5]
            self.logger.info(f"✅ Cliente {client_id} - Top 5 actores: {top_5}")
            self.logger.info(f"✅ Cliente {client_id} - Batches procesados: {self.recovery_manager.batch_processed_counts[client_id]}")
    
    def handle_credits_message(self, message):
        # Usar recovery manager para el resto
        state_data = {
            'save_state': self._save_state,
            'log_state': self._log_state
        }
        
        if self.recovery_manager.process_message(message, state_data, self.process_credits_message, self.send_ack_to_consumer):
            self.logger.info(f"✅ Mensaje procesado exitosamente")
        else:
            self.logger.info(f"⚠️ Mensaje ya procesado o error")
    
    def process_credits_message(self, message):
        """Handler para mensajes de credits"""
        # Verificar si tengo las movies del cliente
        client_id = message.get("client_id")
        if client_id not in self.movies:
            os.makedirs(os.path.dirname(PENDING_MESSAGES), exist_ok=True)
            self.logger.info(f"⏳ Client id {client_id} not ready for credits file. Saving locally")
            with open(PENDING_MESSAGES, "a") as f:
                f.write(json.dumps(message) + "\n")
            return
        
        # Procesar el mensaje
        actors = convert_data(message)
        movies_per_client = self.movies.get(client_id, set())
        self.processed_rating_batches_per_client[client_id] += message.get("batch_size", 0)
        
        # Procesar actores
        if client_id not in self.actor_counts:
            self.actor_counts[client_id] = {}
        
        for actor in actors:
            if actor.movie_id in movies_per_client:
                actor_id = actor.id
                actor_name = actor.name
                if actor_id not in self.actor_counts[client_id]:
                    self.actor_counts[client_id][actor_id] = {"name": actor_name, "count": 1}
                else:
                    self.actor_counts[client_id][actor_id]["count"] += 1
    
    def send_ack_to_consumer(self, batch_id):
        self.credits_consumer.ack(batch_id)
        
    def handle_movies_message(self, message):
        """Handler para mensajes de movies"""
        self.logger.info(f"📽️ Mensaje de movies recibido - cliente: {message.get('client_id')}")
        if message.get("type") == "20_century_arg_total_result":
            self.process_movie_message(message)
        else:
            self.logger.error(f"❌ Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")
    
    def process_movie_message(self, message):
        """Procesa mensaje de movies y reprocesa mensajes pendientes"""
        client_id = message.get("client_id")
        received_movies = {movie["id"] for movie in message.get("movies")}
        
        # Verificar integridad y persistir inmediatamente
        verified_movies = self._verify_movies_integrity(client_id, received_movies)
        self.movies[client_id] = verified_movies
        
        # Persistir movies inmediatamente
        self._persist_movies_immediately(client_id, verified_movies)
        
        self.logger.info(f"📽️ Obtenidas {len(verified_movies)} películas para cliente {client_id}")
        
        # Reprocesar mensajes pendientes
        if os.path.exists(PENDING_MESSAGES):
            temp_path = PENDING_MESSAGES + ".tmp"
            with open(PENDING_MESSAGES, "r") as reading_file, open(temp_path, "w") as writing_file:
                for line in reading_file:
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        self.logger.warning("⚠️ Invalid file line.")
                        continue

                    if msg.get("client_id") == client_id:
                        self.logger.info(f"🔄 Reprocessing message for client {client_id}")
                        self.credits_producer.enqueue(msg)
                    else:
                        writing_file.write(line)

            os.replace(temp_path, PENDING_MESSAGES)
        
        # ✅ MODIFICAR: Usar lógica de reintentos
        if self._ensure_credits_consumer_running():
            self.logger.info("✅ Consumer de credits está ejecutándose correctamente")
        else:
            self.logger.error("❌ No se pudo iniciar consumer de credits")
    
    def get_result(self, client_id):
        """Obtiene el top 10 de actores para un cliente"""
        top_10 = sorted(self.actor_counts[client_id].items(), key=lambda item: item[1]["count"], reverse=True)[:10]
        self.logger.info("✅ Top 10 actores con más contribuciones:")
        return top_10
    
    def close(self):
        """Cierra todas las conexiones"""
        self.logger.info("🔌 Cerrando conexiones del joiner...")
        try:
            self.credits_consumer.close()
            
            # Guardar checkpoint final
            state_data = {
                'save_state': self._save_state,
                'log_state': self._log_state
            }
            self.recovery_manager.save_final_checkpoint(state_data)
            
            # Cerrar recovery manager
            self.recovery_manager.close()
            
            # Cerrar otras conexiones
            self.movies_consumer.close()
            
            self.producer.close()
            self.credits_producer.close()
            
        except Exception as e:
            self.logger.error(f"❌ Error al cerrar conexiones: {e}")
    
    def start(self):
        """Inicia el joiner de credits"""
        self.logger.info("🚀 Iniciando joiner de credits")
        try:
            self.movies_consumer.start()
            
            # Iniciar consumer de credits si ya tenemos movies del checkpoint
            if len(self.movies.keys()) > 0:
                self.logger.info(f"✅ Tengo {len(self.movies.keys())} clientes con movies, iniciando consumer...")
                
                # ✅ AGREGAR: Reprocesar mensajes pendientes después del recovery
                self._reprocess_pending_messages_after_recovery()
                
                # ✅ MODIFICAR: Iniciar consumer con reintentos
                if self._start_credits_consumer_with_retry():
                    self.logger.info("✅ Consumer de credits iniciado exitosamente desde checkpoint")
                else:
                    self.logger.error("❌ No se pudo iniciar consumer de credits desde checkpoint")
            else:
                self.logger.info("⏳ No hay movies para procesar, esperando mensaje de movies")
            
            self.shutdown_event.wait()
        finally:
            self.close()

    def _reprocess_pending_messages_after_recovery(self):
        """Reprocesa mensajes pendientes después del recovery"""
        if os.path.exists(PENDING_MESSAGES):
            self.logger.info("🔄 Reprocesando mensajes pendientes después del recovery...")
            temp_path = PENDING_MESSAGES + ".tmp"
            
            with open(PENDING_MESSAGES, "r") as reading_file, open(temp_path, "w") as writing_file:
                for line in reading_file:
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        self.logger.warning("⚠️ Invalid file line.")
                        continue

                    client_id = msg.get("client_id")
                    if client_id in self.movies:
                        self.logger.info(f"🔄 Reprocesando mensaje pendiente para cliente {client_id}")
                        self.credits_producer.enqueue(msg)
                    else:
                        writing_file.write(line)

            os.replace(temp_path, PENDING_MESSAGES)
            self.logger.info("✅ Reprocesamiento de mensajes pendientes completado")

    def send_batch_id_to_aggregator(self, batch_id):
        msg_dict = {
            "type": "batch_id",
            "batch_id": batch_id,
            "joiner_instance_id": self.joiner_instance_id
        }
        msg = json.dumps(msg_dict) + '\n'
        if self.recovery_manager.tcp_client.send(msg):
            self.logger.info(f"📤 Batch id enviado por TCP: {msg.strip()}")
        else:
            self.logger.error("❌ No se pudo enviar batch id por TCP.")

    # ✅ AGREGAR: Métodos de reintentos
    def _start_credits_consumer_with_retry(self):
        """Inicia el consumer de credits con lógica de reintentos"""
        with self._credits_consumer_lock:
            if self._credits_consumer_started:
                self.logger.info("✅ Consumer de credits ya fue iniciado")
                return True
            
            for attempt in range(self._max_consumer_retries):
                try:
                    self.logger.info(f"Intento {attempt + 1}/{self._max_consumer_retries} de iniciar consumer de credits")
                    
                    # Verificar si el consumer ya está vivo
                    if self.credits_consumer.is_alive():
                        self.logger.info("✅ Consumer de credits ya está vivo")
                        self._credits_consumer_started = True
                        return True
                    
                    # Intentar iniciar el consumer
                    self.credits_consumer.start_consuming_2()
                    
                    # Esperar un poco para que se estabilice
                    time.sleep(1)
                    
                    # Verificar que se inició correctamente
                    if self.credits_consumer.is_alive():
                        self.logger.info("✅ Consumer de credits iniciado exitosamente")
                        self._credits_consumer_started = True
                        self._consumer_retry_count = 0  # Reset contador de reintentos
                        return True
                    else:
                        self.logger.warning(f"⚠️ Consumer de credits no está vivo después del intento {attempt + 1}")
                        
                except Exception as e:
                    self.logger.error(f"❌ Error iniciando consumer de credits (intento {attempt + 1}): {e}")
                    
                    # Si es error de reentrancia, esperar más tiempo
                    if "ReentrancyError" in str(e) or "start_consuming" in str(e):
                        self.logger.info("🔄 Error de reentrancia detectado, esperando más tiempo...")
                        time.sleep(self._consumer_retry_delay * 2)
                    else:
                        time.sleep(self._consumer_retry_delay)
                
                self._consumer_retry_count += 1
            
            self.logger.error(f"❌ No se pudo iniciar consumer de credits después de {self._max_consumer_retries} intentos")
            return False

    def _ensure_credits_consumer_running(self):
        """Asegura que el consumer de credits esté ejecutándose"""
        if not self._credits_consumer_started or not self.credits_consumer.is_alive():
            self.logger.info("🔄 Consumer de credits no está ejecutándose, intentando iniciar...")
            return self._start_credits_consumer_with_retry()
        return True

if __name__ == '__main__':
    worker = CreditsJoinerSimple()
    worker.start() 
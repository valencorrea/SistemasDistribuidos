import json
import os
from collections import defaultdict
import logging
import time
import threading

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from worker.worker import Worker
from middleware.tcp_protocol.tcp_protocol import TCPClient

from joiner.base.joiner_recovery_manager import JoinerRecoveryManager
from utils.parsers.ratings_parser import convert_data_for_rating_joiner

PENDING_MESSAGES = "/root/files/ratings_pending.jsonl"
BATCH_PERSISTENCE_DIR = "/root/files/ratings_batches/"
CHECKPOINT_FILE = "/root/files/ratings_checkpoint.json"
MOVIES_PERSISTENCE_FILE = "/root/files/ratings_movies.json"  # Nuevo archivo para persistir movies

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class RatingsJoiner(Worker):
    """RatingsJoiner usando JoinerRecoveryManager"""
    
    def __init__(self):
        super().__init__()
        
        # Estado específico del ratings joiner
        self.rating_counts = {}
        self.movies = {}
        self.processed_rating_batches_per_client = defaultdict(int)
        
        # Variables para control de reintentos
        self._ratings_consumer_started = False
        self._ratings_consumer_lock = threading.Lock()
        self._consumer_retry_count = 0
        self._max_consumer_retries = 3
        self._consumer_retry_delay = 2  # segundos
        
        # Configurar recovery manager
        config = {
            'checkpoint_file': CHECKPOINT_FILE,
            'batch_persistence_dir': BATCH_PERSISTENCE_DIR,
            'aggregator_queue': 'ratings_aggregator',
            'aggregator_response_queue': 'ratings_joiner_response',
            'joiner_id': 'RatingsJoiner',
            'checkpoint_interval': 10,  # REDUCIDO: checkpoint cada 10 mensajes
            'log_interval': 5,          # REDUCIDO: log cada 5 mensajes
            'aggregator_host': os.getenv("RATINGS_AGGREGATOR_HOST", "best_and_worst_ratings_aggregator"),
            'aggregator_port': int(os.getenv("RATINGS_AGGREGATOR_PORT", 60001))
        }
        
        self.recovery_manager = JoinerRecoveryManager(config, self.logger)
        
        # Consumers y producers específicos del ratings joiner
        self.movies_consumer = Subscriber("20_century_arg_result",
                                        message_handler=self.handle_movies_message,
                                        durable=False,  # ✅ NO cambiar configuración del exchange
                                        auto_ack=False)  # ✅ CONFIGURACIÓN ROBUSTA para ACK
        self.ratings_consumer = Consumer("ratings",
                                       _message_handler=self.handle_ratings_message)
        self.producer = Producer("top_10_ratings_from_batch")
        self.ratings_producer = Producer(
            queue_name="ratings",
            queue_type="direct")
        self.joiner_instance_id = "joiner_ratings"
        
        # Cargar estado inicial
        self._load_initial_state()
    
    def _load_initial_state(self):
        """Carga estado inicial usando el recovery manager"""
        # Primero cargar movies desde persistencia
        self._load_movies_from_persistence()
        
        state_data = {
            'checkpoint_file': CHECKPOINT_FILE,
            'batch_persistence_dir': BATCH_PERSISTENCE_DIR,
            'aggregator_queue': 'ratings_aggregator',
            'aggregator_response_queue': 'ratings_joiner_response',
            'joiner_id': 'RatingsJoiner',
            'checkpoint_interval': 10,
            'log_interval': 5,
            'aggregator_host': os.getenv("AGGREGATOR_HOST", "best_and_worst_ratings_aggregator"),
            'aggregator_port': int(os.getenv("AGGREGATOR_PORT", 60001)),
            'restore_state': self._restore_state,
            'save_state': self._save_state,
            'log_state': self._log_state,
            'process_message': self.process_ratings_message
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
                
                self.logger.info(f" Recuperadas movies de {len(self.movies)} clientes desde persistencia")
                
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
            self.logger.info(f" Movies del cliente {client_id} persistidas inmediatamente ({len(movies)} películas)")
            
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
        """Restaura estado específico del ratings joiner"""
        self.rating_counts = checkpoint_data.get("rating_counts", {})
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
        
        self.logger.info(f"✅ Estado restaurado: {len(self.rating_counts)} clientes con datos, {len(self.movies)} clientes con movies")
    
    def _save_state(self):
        """Guarda estado específico del ratings joiner"""
        return {
            "rating_counts": self.rating_counts,
            "processed_rating_batches_per_client": dict(self.processed_rating_batches_per_client),
            "movies": {client_id: list(movie_ids) for client_id, movie_ids in self.movies.items()}
        }
    
    def _log_state(self):
        """Log de estado específico del ratings joiner"""
        self.logger.info(f" Clientes con datos: {list(self.rating_counts.keys())}")
        self.logger.info(f"📽️ Clientes con movies: {list(self.movies.keys())}")
        
        for client_id, rating_data in self.rating_counts.items():
            top_5 = sorted(rating_data.items(), key=lambda item: item[1]["count"], reverse=True)[:5]
            self.logger.info(f"⭐ Cliente {client_id} - Top 5 ratings: {top_5}")
            self.logger.info(f" Cliente {client_id} - Batches procesados: {self.recovery_manager.batch_processed_counts[client_id]}")
    
    def handle_ratings_message(self, message):
        # Usar recovery manager para el resto
        state_data = {
            'save_state': self._save_state,
            'log_state': self._log_state
        }
        
        if self.recovery_manager.process_message(message, state_data, self.process_ratings_message, self.send_ack_to_consumer):
            self.logger.info(f"✅ Mensaje procesado exitosamente")
        else:
            self.logger.info(f"⚠️ Mensaje ya procesado o error")
    
    def process_ratings_message(self, message):
        """Handler para mensajes de ratings"""
        client_id = message.get("client_id")
        
        # Verificar si tengo las movies del cliente
        if client_id not in self.movies:
            os.makedirs(os.path.dirname(PENDING_MESSAGES), exist_ok=True)
            self.logger.info(f"⏳ Client id {client_id} not ready for ratings file. Saving locally")
            with open(PENDING_MESSAGES, "a") as f:
                f.write(json.dumps(message) + "\n")
            return
        
        # Procesar el mensaje (lógica específica del ratings joiner)
        ratings = self._parse_ratings(message)
        self.processed_rating_batches_per_client[client_id] += message.get("batch_size", 0)
        
        # Inicializar rating_counts si no existe
        if client_id not in self.rating_counts:
            self.rating_counts[client_id] = {}
        
        # Procesar ratings
        for rating in ratings:
            if not isinstance(rating, dict):
                continue
            movie_id = rating.get("movieId")
            rating_value = float(rating.get("rating", 0))
            
            # Verificar si la película está en las movies del cliente
            if int(movie_id) in self.movies[client_id] or str(movie_id) in self.movies[client_id]:
                self.logger.debug(f"⭐ Se agrega rating a la pelicula {movie_id}")
                
                # Contar ratings por valor
                if rating_value not in self.rating_counts[client_id]:
                    self.rating_counts[client_id][rating_value] = {"count": 1}
                else:
                    self.rating_counts[client_id][rating_value]["count"] += 1
    
    def send_ack_to_consumer(self, batch_id):
        self.ratings_consumer.ack(batch_id)
    
    def _parse_ratings(self, message):
        """Función específica para parsear ratings"""
        return convert_data_for_rating_joiner(message)
    
    def handle_movies_message(self, message):
        """Handler para mensajes de movies"""
        self.logger.info(f"️ Mensaje de movies recibido - cliente: {message.get('client_id')}")
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
                        self.ratings_producer.enqueue(msg)
                    else:
                        writing_file.write(line)

            os.replace(temp_path, PENDING_MESSAGES)
        
        # Asegurar que el consumer de ratings esté ejecutándose
        if self._ensure_ratings_consumer_running():
            self.logger.info("✅ Consumer de ratings está ejecutándose correctamente")
        else:
            self.logger.error("❌ No se pudo iniciar consumer de ratings")
    
    def get_result(self, client_id):
        """Obtiene el top 10 de ratings para un cliente"""
        top_10 = sorted(self.rating_counts[client_id].items(), key=lambda item: item[1]["count"], reverse=True)[:10]
        self.logger.info(" Top 10 ratings con más frecuencia:")
        return top_10
    
    def get_movies_with_votes_for_client(self, client_id):
        movies_with_ratings = {}
        for movie_id, data in self.movies_ratings[client_id].items():
            if data["votes"] > 0:
                movies_with_ratings[movie_id] = data
        self.logger.info(f"📽️ Obtenidas {len(movies_with_ratings.keys())} peliculas con al menos un rating")
        return movies_with_ratings
    
    def close(self):
        """Cierra todas las conexiones"""
        self.logger.info("🔌 Cerrando conexiones del joiner...")
        try:
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
            self.ratings_consumer.close()
            self.producer.close()
            self.ratings_producer.close()
            
        except Exception as e:
            self.logger.error(f"❌ Error al cerrar conexiones: {e}")
    
    def _start_ratings_consumer_with_retry(self):
        """Inicia el consumer de ratings con lógica de reintentos"""
        with self._ratings_consumer_lock:
            if self._ratings_consumer_started:
                self.logger.info("✅ Consumer de ratings ya fue iniciado")
                return True
            
            for attempt in range(self._max_consumer_retries):
                try:
                    self.logger.info(f" Intento {attempt + 1}/{self._max_consumer_retries} de iniciar consumer de ratings")
                    
                    # Verificar si el consumer ya está vivo
                    if self.ratings_consumer.is_alive():
                        self.logger.info("✅ Consumer de ratings ya está vivo")
                        self._ratings_consumer_started = True
                        return True
                    
                    # Intentar iniciar el consumer
                    self.ratings_consumer.start_consuming_2()
                    
                    # Esperar un poco para que se estabilice
                    time.sleep(1)
                    
                    # Verificar que se inició correctamente
                    if self.ratings_consumer.is_alive():
                        self.logger.info("✅ Consumer de ratings iniciado exitosamente")
                        self._ratings_consumer_started = True
                        self._consumer_retry_count = 0  # Reset contador de reintentos
                        return True
                    else:
                        self.logger.warning(f"⚠️ Consumer de ratings no está vivo después del intento {attempt + 1}")
                        
                except Exception as e:
                    self.logger.error(f"❌ Error iniciando consumer de ratings (intento {attempt + 1}): {e}")
                    
                    # Si es error de reentrancia, esperar más tiempo
                    if "ReentrancyError" in str(e) or "start_consuming" in str(e):
                        self.logger.info("🔄 Error de reentrancia detectado, esperando más tiempo...")
                        time.sleep(self._consumer_retry_delay * 2)
                    else:
                        time.sleep(self._consumer_retry_delay)
                
                self._consumer_retry_count += 1
            
            self.logger.error(f"❌ No se pudo iniciar consumer de ratings después de {self._max_consumer_retries} intentos")
            return False

    def _ensure_ratings_consumer_running(self):
        """Asegura que el consumer de ratings esté ejecutándose"""
        if not self._ratings_consumer_started or not self.ratings_consumer.is_alive():
            self.logger.info("🔄 Consumer de ratings no está ejecutándose, intentando iniciar...")
            return self._start_ratings_consumer_with_retry()
        return True

    def start(self):
        """Inicia el joiner de ratings"""
        self.logger.info("🚀 Iniciando joiner de ratings")
        try:
            self.movies_consumer.start()
            
            # Iniciar consumer de ratings si ya tenemos movies del checkpoint
            if len(self.movies.keys()) > 0:
                self.logger.info(f"✅ Tengo {len(self.movies.keys())} clientes con movies, iniciando consumer...")
                
                # ✅ AGREGAR: Reprocesar mensajes pendientes después del recovery
                self._reprocess_pending_messages_after_recovery()
                
                # Iniciar consumer con reintentos
                if self._start_ratings_consumer_with_retry():
                    self.logger.info("✅ Consumer de ratings iniciado exitosamente desde checkpoint")
                else:
                    self.logger.error("❌ No se pudo iniciar consumer de ratings desde checkpoint")
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
                        self.logger.warning("⚠️ Línea inválida en archivo pendiente.")
                        continue

                    client_id = msg.get("client_id")
                    if client_id in self.movies:
                        self.logger.info(f"🔄 Reprocesando mensaje pendiente para cliente {client_id}")
                        self.ratings_producer.enqueue(msg)
                    else:
                        writing_file.write(line)

            os.replace(temp_path, PENDING_MESSAGES)
            self.logger.info("✅ Reprocesamiento de mensajes pendientes completado")


if __name__ == '__main__':
    worker = RatingsJoiner()
    worker.start() 
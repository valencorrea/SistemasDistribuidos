import json
import os
from collections import defaultdict
import logging

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
        
        # Configurar recovery manager
        config = {
            'checkpoint_file': CHECKPOINT_FILE,
            'batch_persistence_dir': BATCH_PERSISTENCE_DIR,
            'aggregator_queue': 'ratings_aggregator',
            'aggregator_response_queue': 'ratings_joiner_response',
            'joiner_id': 'RatingsJoiner',
            'checkpoint_interval': 10,  # REDUCIDO: checkpoint cada 10 mensajes
            'log_interval': 5,          # REDUCIDO: log cada 5 mensajes
            'aggregator_host': 'ratings_aggregator',
            'aggregator_port': 50000
        }
        
        self.recovery_manager = JoinerRecoveryManager(config, self.logger)
        
        # Consumers y producers específicos del ratings joiner
        self.movies_consumer = Subscriber("20_century_arg_result",
                                        message_handler=self.handle_movies_message)
        self.ratings_consumer = Consumer("ratings",
                                        _message_handler=self.handle_ratings_message)
        self.producer = Producer("top_10_ratings_from_batch")
        self.ratings_producer = Producer(
            queue_name="ratings",
            queue_type="direct")
        self.joiner_instance_id = "joiner_ratings"
        host = os.getenv("RATINGS_AGGREGATOR_HOST", "ratings_aggregator")
        port = int(os.getenv("RATINGS_AGGREGATOR_PORT", 60001))
        self.tcp_client = TCPClient(host, port)
        
        # Cargar estado inicial
        self._load_initial_state()
    
    def _load_initial_state(self):
        """Carga estado inicial usando el recovery manager"""
        state_data = {
            'checkpoint_file': CHECKPOINT_FILE,
            'batch_persistence_dir': BATCH_PERSISTENCE_DIR,
            'aggregator_queue': 'ratings_aggregator',
            'aggregator_response_queue': 'ratings_joiner_response',
            'joiner_id': 'RatingsJoiner',
            'checkpoint_interval': 10,
            'log_interval': 5,
            'aggregator_host': 'ratings_aggregator',
            'aggregator_port': 50000,
            'restore_state': self._restore_state,
            'save_state': self._save_state,
            'log_state': self._log_state,
            'process_message': self.process_ratings_message
        }
        self.recovery_manager.load_checkpoint_and_recover(state_data)
    
    def _restore_state(self, checkpoint_data):
        """Restaura estado específico del ratings joiner"""
        self.rating_counts = checkpoint_data.get("rating_counts", {})
        self.processed_rating_batches_per_client = defaultdict(int, checkpoint_data.get("processed_rating_batches_per_client", {}))
        
        # Restaurar movies (convertir listas de vuelta a sets)
        movies_data = checkpoint_data.get("movies", {})
        self.movies = {client_id: set(movie_ids) for client_id, movie_ids in movies_data.items()}
    
    def _save_state(self):
        """Guarda estado específico del ratings joiner"""
        return {
            "rating_counts": self.rating_counts,
            "processed_rating_batches_per_client": dict(self.processed_rating_batches_per_client),
            "movies": {client_id: list(movie_ids) for client_id, movie_ids in self.movies.items()}
        }
    
    def _log_state(self):
        """Log de estado específico del ratings joiner"""
        self.logger.info(f"Clientes con datos: {list(self.rating_counts.keys())}")
        
        for client_id, rating_data in self.rating_counts.items():
            top_5 = sorted(rating_data.items(), key=lambda item: item[1]["count"], reverse=True)[:5]
            self.logger.info(f"Cliente {client_id} - Top 5 ratings: {top_5}")
            self.logger.info(f"Cliente {client_id} - Batches procesados: {self.recovery_manager.batch_processed_counts[client_id]}")
    
    def handle_ratings_message(self, message):
        # Usar recovery manager para el resto
        state_data = {
            'save_state': self._save_state,
            'log_state': self._log_state
        }
        
        if self.recovery_manager.process_message(message, state_data, self.process_ratings_message, self.send_ack_to_consumer):
            self.logger.info(f"Mensaje procesado exitosamente")
        else:
            self.logger.info(f"Mensaje ya procesado o error")
    
    def process_ratings_message(self, message):
        """Handler para mensajes de ratings"""
        client_id = message.get("client_id")
        
        # Verificar si tengo las movies del cliente
        if client_id not in self.movies:
            os.makedirs(os.path.dirname(PENDING_MESSAGES), exist_ok=True)
            self.logger.info(f"Client id {client_id} not ready for ratings file. Saving locally")
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
                self.logger.debug(f"Se agrega rating a la pelicula {movie_id}")
                
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
        self.logger.info(f"Mensaje de movies recibido - cliente: {message.get('client_id')}")
        if message.get("type") == "20_century_arg_total_result":
            self.process_movie_message(message)
        else:
            self.logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")
    
    def process_movie_message(self, message):
        """Procesa mensaje de movies y reprocesa mensajes pendientes"""
        client_id = message.get("client_id")
        self.movies[client_id] = {movie["id"] for movie in message.get("movies")}
        self.logger.info(f"Obtenidas {message.get('total_movies')} películas")
        
        # Reprocesar mensajes pendientes
        if os.path.exists(PENDING_MESSAGES):
            temp_path = PENDING_MESSAGES + ".tmp"
            with open(PENDING_MESSAGES, "r") as reading_file, open(temp_path, "w") as writing_file:
                for line in reading_file:
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        self.logger.warning("Invalid file line.")
                        continue

                    if msg.get("client_id") == client_id:
                        self.logger.info(f"Reprocessing message for client {client_id}")
                        self.ratings_producer.enqueue(msg)
                    else:
                        writing_file.write(line)

            os.replace(temp_path, PENDING_MESSAGES)
        
        # Iniciar consumer de ratings si no está vivo
        if not self.ratings_consumer.is_alive():
            self.ratings_consumer.start_consuming_2()
            self.logger.info("Thread de consumo de ratings empezado")
        else:
            self.logger.info("Thread de consumo de ratings no empezado, ya existe uno")
    
    def get_result(self, client_id):
        """Obtiene el top 10 de ratings para un cliente"""
        top_10 = sorted(self.rating_counts[client_id].items(), key=lambda item: item[1]["count"], reverse=True)[:10]
        self.logger.info("Top 10 ratings con más frecuencia:")
        return top_10
    
    def get_movies_with_votes_for_client(self, client_id):
        movies_with_ratings = {}
        for movie_id, data in self.movies_ratings[client_id].items():
            if data["votes"] > 0:
                movies_with_ratings[movie_id] = data
        self.logger.info(f"Obtenidas {len(movies_with_ratings.keys())} peliculas con al menos un rating")
        return movies_with_ratings
    
    def close(self):
        """Cierra todas las conexiones"""
        self.logger.info("Cerrando conexiones del joiner...")
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
            self.logger.error(f"Error al cerrar conexiones: {e}")
    
    def start(self):
        """Inicia el joiner de ratings"""
        self.logger.info("Iniciando joiner de ratings")
        try:
            self.movies_consumer.start()
            
            # Iniciar consumer de ratings si ya tenemos movies del checkpoint
            if len(self.movies.keys()) > 0:
                self.logger.info(f"Tengo {len(self.movies.keys())} clientes con movies, iniciando consumer...")
                
                # ✅ AGREGAR: Reprocesar mensajes pendientes después del recovery
                self._reprocess_pending_messages_after_recovery()
                
                self.ratings_consumer.start_consuming_2()
                self.logger.info("Thread de consumo de ratings empezado desde checkpoint")
            else:
                self.logger.info("No hay movies para procesar, esperando mensaje de movies")
            
            self.shutdown_event.wait()
        finally:
            self.close()

    def _reprocess_pending_messages_after_recovery(self):
        """Reprocesa mensajes pendientes después del recovery"""
        if os.path.exists(PENDING_MESSAGES):
            self.logger.info("Reprocesando mensajes pendientes después del recovery...")
            temp_path = PENDING_MESSAGES + ".tmp"
            
            with open(PENDING_MESSAGES, "r") as reading_file, open(temp_path, "w") as writing_file:
                for line in reading_file:
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        self.logger.warning("Línea inválida en archivo pendiente.")
                        continue

                client_id = msg.get("client_id")
                if client_id in self.movies:
                    self.logger.info(f"Reprocesando mensaje pendiente para cliente {client_id}")
                    self.ratings_producer.enqueue(msg)
                else:
                    writing_file.write(line)

            os.replace(temp_path, PENDING_MESSAGES)
            self.logger.info("Reprocesamiento de mensajes pendientes completado")


if __name__ == '__main__':
    worker = RatingsJoiner()
    worker.start() 
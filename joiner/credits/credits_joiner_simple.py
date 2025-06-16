import json
import os
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from utils.parsers.credits_parser import convert_data
from worker.worker import Worker

from joiner.base.joiner_recovery_manager import JoinerRecoveryManager

PENDING_MESSAGES = "/root/files/credits_pending.jsonl"
BATCH_PERSISTENCE_DIR = "/root/files/credits_batches/"
CHECKPOINT_FILE = "/root/files/credits_checkpoint.json"


class CreditsJoinerSimple(Worker):
    """CreditsJoiner usando JoinerRecoveryManager"""
    
    def __init__(self):
        super().__init__()
        
        # Estado específico del credits joiner
        self.actor_counts = {}
        self.movies = {}
        self.processed_rating_batches_per_client = defaultdict(int)
        
        # Configurar recovery manager
        config = {
            'checkpoint_file': CHECKPOINT_FILE,
            'batch_persistence_dir': BATCH_PERSISTENCE_DIR,
            'aggregator_queue': 'credits_aggregator',
            'aggregator_response_queue': 'credits_joiner_response',
            'joiner_id': 'CreditsJoiner',
            'checkpoint_interval': 500,
            'log_interval': 100
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
        
        # Cargar estado inicial
        self._load_initial_state()
    
    def _load_initial_state(self):
        """Carga estado inicial usando el recovery manager"""
        state_data = {
            'restore_state': self._restore_state,
            'save_state': self._save_state,
            'log_state': self._log_state
        }
        self.recovery_manager.load_checkpoint_and_recover(state_data)
    
    def _restore_state(self, checkpoint_data):
        """Restaura estado específico del credits joiner"""
        self.actor_counts = checkpoint_data.get("actor_counts", {})
        self.processed_rating_batches_per_client = defaultdict(int, checkpoint_data.get("processed_rating_batches_per_client", {}))
        
        # Restaurar movies (convertir listas de vuelta a sets)
        movies_data = checkpoint_data.get("movies", {})
        self.movies = {client_id: set(movie_ids) for client_id, movie_ids in movies_data.items()}
    
    def _save_state(self):
        """Guarda estado específico del credits joiner"""
        return {
            "actor_counts": self.actor_counts,
            "processed_rating_batches_per_client": dict(self.processed_rating_batches_per_client),
            "movies": {client_id: list(movie_ids) for client_id, movie_ids in self.movies.items()}
        }
    
    def _log_state(self):
        """Log de estado específico del credits joiner"""
        self.logger.info(f"Clientes con datos: {list(self.actor_counts.keys())}")
        
        for client_id, actor_data in self.actor_counts.items():
            top_5 = sorted(actor_data.items(), key=lambda item: item[1]["count"], reverse=True)[:5]
            self.logger.info(f"Cliente {client_id} - Top 5 actores: {top_5}")
            self.logger.info(f"Cliente {client_id} - Batches procesados: {self.recovery_manager.batch_processed_counts[client_id]}")
    
    def handle_credits_message(self, message):
        """Handler para mensajes de credits"""
        # Verificar si tengo las movies del cliente
        client_id = message.get("client_id")
        if client_id not in self.movies:
            os.makedirs(os.path.dirname(PENDING_MESSAGES), exist_ok=True)
            self.logger.info(f"Client id {client_id} not ready for credits file. Saving locally")
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
        
        # Usar recovery manager para el resto
        state_data = {
            'save_state': self._save_state,
            'log_state': self._log_state
        }
        
        if self.recovery_manager.process_message(message, state_data):
            self.logger.info(f"Mensaje procesado exitosamente")
        else:
            self.logger.info(f"Mensaje ya procesado o error")
    
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
                        self.credits_producer.enqueue(msg)
                    else:
                        writing_file.write(line)

            os.replace(temp_path, PENDING_MESSAGES)
        
        # Iniciar consumer de credits si no está vivo
        if not self.credits_consumer.is_alive():
            self.credits_consumer.start()
            self.logger.info("Thread de consumo de credits empezado")
        else:
            self.logger.info("Thread de consumo de credits no empezado, ya existe uno")
    
    def get_result(self, client_id):
        """Obtiene el top 10 de actores para un cliente"""
        top_10 = sorted(self.actor_counts[client_id].items(), key=lambda item: item[1]["count"], reverse=True)[:10]
        self.logger.info("Top 10 actores con más contribuciones:")
        return top_10
    
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
            self.credits_consumer.close()
            self.producer.close()
            self.credits_producer.close()
            
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")
    
    def start(self):
        """Inicia el joiner de credits"""
        self.logger.info("Iniciando joiner de credits")
        try:
            self.movies_consumer.start()
            self.shutdown_event.wait()
        finally:
            self.close()


if __name__ == '__main__':
    worker = CreditsJoinerSimple()
    worker.start() 
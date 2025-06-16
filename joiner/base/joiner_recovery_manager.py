import json
import logging
import os
import time
import threading
from collections import defaultdict

from middleware.producer.producer import Producer
from middleware.consumer.subscriber import Subscriber


class JoinerRecoveryManager:
    """Maneja recuperación y checkpoint para joiners"""
    
    def __init__(self, config, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        
        # Configuración
        self.checkpoint_file = config['checkpoint_file']
        self.batch_persistence_dir = config['batch_persistence_dir']
        self.aggregator_queue = config['aggregator_queue']
        self.aggregator_response_queue = config['aggregator_response_queue']
        self.joiner_id = config['joiner_id']
        self.checkpoint_interval = config.get('checkpoint_interval', 500)
        self.log_interval = config.get('log_interval', 100)
        
        # Estado
        self.processed_messages = set()
        self.batch_processed_counts = defaultdict(int)
        self.messages_since_last_checkpoint = 0
        self.messages_since_last_log = 0
        
        # Conexiones
        self.aggregator_producer = Producer(self.aggregator_queue)
        self.aggregator_consumer = Subscriber(self.aggregator_response_queue, 
                                             message_handler=self._handle_aggregator_response)
        
        # Variables para recuperación
        self.pending_aggregator_queries = {}
        self.query_timeout = 30
        
        # Crear directorio de persistencia
        os.makedirs(self.batch_persistence_dir, exist_ok=True)
    
    def start(self):
        """Inicia el consumer del aggregator"""
        self.aggregator_consumer.start()
        self.logger.info("Consumer del aggregator iniciado")
    
    def close(self):
        """Cierra conexiones"""
        try:
            self.aggregator_consumer.close()
            self.aggregator_producer.close()
        except Exception as e:
            self.logger.error(f"Error cerrando conexiones: {e}")
    
    def load_checkpoint_and_recover(self, state_data):
        """Carga checkpoint y hace recuperación"""
        # 1. Cargar checkpoint
        self._load_checkpoint(state_data)
        
        # 2. Recuperar desde archivos de batch
        self._recover_from_batch_files()
        
        # 3. Iniciar consumer del aggregator
        self.start()
    
    def _load_checkpoint(self, state_data):
        """Carga checkpoint del estado"""
        if not os.path.exists(self.checkpoint_file):
            self.logger.info("No se encontró checkpoint, iniciando desde cero")
            return
        
        try:
            with open(self.checkpoint_file, "r") as f:
                checkpoint_data = json.load(f)
            
            # Restaurar estado básico
            self.processed_messages = set(checkpoint_data.get("processed_messages", []))
            self.batch_processed_counts = defaultdict(int, checkpoint_data.get("batch_processed_counts", {}))
            
            # Restaurar estado específico del joiner
            if 'restore_state' in state_data:
                state_data['restore_state'](checkpoint_data)
            
            timestamp = checkpoint_data.get("timestamp", 0)
            self.logger.info(f"Checkpoint cargado desde {time.ctime(timestamp)}")
            self.logger.info(f"Estado restaurado: {len(self.processed_messages)} mensajes procesados")
            
        except Exception as e:
            self.logger.error(f"Error al cargar checkpoint: {e}")
    
    def _recover_from_batch_files(self):
        """Recupera estado consultando archivos de batch"""
        if not os.path.exists(self.batch_persistence_dir):
            self.logger.info("No hay archivos de batch para recuperar")
            return
        
        self.logger.info("Iniciando recuperación desde archivos de batch...")
        
        batch_files = [f for f in os.listdir(self.batch_persistence_dir) 
                      if f.endswith('.json') and not f.endswith('_END_TRX.json')]
        
        for batch_file in batch_files:
            try:
                parts = batch_file.replace('.json', '').split('_')
                if len(parts) < 2:
                    continue
                
                batch_id = parts[-1]
                client_id = '_'.join(parts[:-1])
                message_id = f"{client_id}_{batch_id}"
                
                # Verificar END_TRX
                end_trx_file = os.path.join(self.batch_persistence_dir, f"{client_id}_{batch_id}_END_TRX.json")
                
                if os.path.exists(end_trx_file):
                    self.logger.info(f"Batch {message_id} tiene END_TRX, asumiendo procesado")
                    self.processed_messages.add(message_id)
                else:
                    self.logger.info(f"Batch {message_id} sin END_TRX, consultando al aggregator...")
                    self._query_aggregator_for_batch(client_id, batch_id, batch_file)
                    
            except Exception as e:
                self.logger.error(f"Error procesando archivo {batch_file}: {e}")
        
        self._wait_for_aggregator_responses()
    
    def _query_aggregator_for_batch(self, client_id, batch_id, batch_file):
        """Consulta al aggregator si ya procesó este batch"""
        query_message = {
            "type": "batch_status_query",
            "client_id": client_id,
            "batch_id": batch_id,
            "joiner_id": self.joiner_id,
            "batch_file": batch_file
        }
        
        self.pending_aggregator_queries[f"{client_id}_{batch_id}"] = batch_file
        self.aggregator_producer.enqueue(query_message)
        self.logger.info(f"Consulta enviada al aggregator: {query_message}")
    
    def _wait_for_aggregator_responses(self):
        """Espera las respuestas del aggregator"""
        if not self.pending_aggregator_queries:
            return
        
        self.logger.info(f"Esperando {len(self.pending_aggregator_queries)} respuestas del aggregator...")
        
        start_time = time.time()
        while self.pending_aggregator_queries and (time.time() - start_time) < self.query_timeout:
            time.sleep(1)
        
        if self.pending_aggregator_queries:
            self.logger.warning(f"Timeout esperando respuestas del aggregator")
    
    def _handle_aggregator_response(self, message):
        """Maneja respuestas del aggregator"""
        try:
            response_type = message.get("type")
            client_id = message.get("client_id")
            batch_id = message.get("batch_id")
            message_id = f"{client_id}_{batch_id}"
            
            self.logger.info(f"Respuesta del aggregator: {response_type} para {message_id}")
            
            if response_type == "batch_processed_by_me":
                self.logger.info(f"Batch {message_id} fue procesado por mí")
                self.processed_messages.add(message_id)
            elif response_type == "batch_processed_by_other":
                self.logger.info(f"Batch {message_id} fue procesado por otro joiner")
            
            if message_id in self.pending_aggregator_queries:
                del self.pending_aggregator_queries[message_id]
                
        except Exception as e:
            self.logger.error(f"Error procesando respuesta del aggregator: {e}")
    
    def process_message(self, message, state_data):
        """Procesa un mensaje con recuperación y control"""
        # 1. Check O(1) si ya procesé
        message_id = f"{message.get('client_id')}_{message.get('batch_id', 'unknown')}"
        if message_id in self.processed_messages:
            self.logger.info(f"Mensaje ya procesado: {message_id}")
            return False
        
        client_id = message.get("client_id")
        batch_id = message.get("batch_id", "unknown")
        
        try:
            # 2. Persistir resultado
            self._persist_batch_result(client_id, batch_id, message, state_data)
            
            # 3. Actualizar contadores
            self.batch_processed_counts[client_id] += 1
            
            # 4. Enviar control al aggregator
            self._send_batch_processed(client_id, batch_id, message)
            
            # 5. Marcar como procesado
            self.processed_messages.add(message_id)
            
            # 6. Persistir END_TRX
            self._persist_end_transaction(client_id, batch_id)
            
            # 7. Logs y checkpoints
            self._handle_periodic_operations(state_data)
            
            return True
            
        except Exception as e:
            self.logger.exception(f"Error procesando mensaje: {e}")
            return False
    
    def _persist_batch_result(self, client_id, batch_id, message, state_data):
        """Persiste resultado del batch"""
        batch_file = os.path.join(self.batch_persistence_dir, f"{client_id}_{batch_id}.json")
        batch_data = {
            "client_id": client_id,
            "batch_id": batch_id,
            "timestamp": time.time(),
            "message": message,
            "state_data": state_data
        }
        
        with open(batch_file, "w") as f:
            json.dump(batch_data, f, indent=2)
        
        self.logger.info(f"Batch persistido: {batch_file}")
    
    def _persist_end_transaction(self, client_id, batch_id):
        """Persiste END_TRX"""
        end_trx_file = os.path.join(self.batch_persistence_dir, f"{client_id}_{batch_id}_END_TRX.json")
        end_trx_data = {
            "client_id": client_id,
            "batch_id": batch_id,
            "timestamp": time.time(),
            "status": "completed"
        }
        
        with open(end_trx_file, "w") as f:
            json.dump(end_trx_data, f, indent=2)
        
        self.logger.info(f"END_TRX persistido: {end_trx_file}")
    
    def _send_batch_processed(self, client_id, batch_id, message):
        """Envía mensaje de control al aggregator"""
        control_message = {
            "type": "batch_processed",
            "client_id": client_id,
            "batch_id": batch_id,
            "processed_batches": self.batch_processed_counts[client_id],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0)
        }
        self.aggregator_producer.enqueue(control_message)
        self.logger.info(f"Control enviado al aggregator: {control_message}")
    
    def _handle_periodic_operations(self, state_data):
        """Maneja operaciones periódicas (logs y checkpoints)"""
        # Log de control
        self.messages_since_last_log += 1
        if self.messages_since_last_log >= self.log_interval:
            self._log_current_state(state_data)
            self.messages_since_last_log = 0
        
        # Checkpoint
        self.messages_since_last_checkpoint += 1
        if self.messages_since_last_checkpoint >= self.checkpoint_interval:
            self._save_checkpoint(state_data)
            self.messages_since_last_checkpoint = 0
    
    def _log_current_state(self, state_data):
        """Log de control"""
        self.logger.info("=== ESTADO ACTUAL DEL JOINER ===")
        self.logger.info(f"Total de mensajes procesados: {len(self.processed_messages)}")
        
        if 'log_state' in state_data:
            state_data['log_state']()
        
        self.logger.info("=== FIN ESTADO ACTUAL ===")
    
    def _save_checkpoint(self, state_data):
        """Guarda checkpoint"""
        checkpoint_data = {
            "timestamp": time.time(),
            "processed_messages": list(self.processed_messages),
            "batch_processed_counts": dict(self.batch_processed_counts)
        }
        
        # Agregar estado específico del joiner
        if 'save_state' in state_data:
            checkpoint_data.update(state_data['save_state']())
        
        # Guardar de forma atómica
        temp_checkpoint = self.checkpoint_file + ".tmp"
        with open(temp_checkpoint, "w") as f:
            json.dump(checkpoint_data, f, indent=2)
        
        os.replace(temp_checkpoint, self.checkpoint_file)
        self.logger.info(f"Checkpoint guardado: {self.checkpoint_file}")
    
    def save_final_checkpoint(self, state_data):
        """Guarda checkpoint final antes de cerrar"""
        if len(self.processed_messages) > 0:
            self.logger.info("Guardando checkpoint final antes de cerrar...")
            self._save_checkpoint(state_data) 
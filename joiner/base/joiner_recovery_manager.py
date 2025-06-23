import json
import logging
import os
import time
import socket
from collections import defaultdict

from middleware.producer.producer import Producer
from middleware.consumer.subscriber import Subscriber
from middleware.tcp_protocol.tcp_protocol import TCPClient


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
        self.checkpoint_interval = config.get('checkpoint_interval', 5)
        self.log_interval = config.get('log_interval', 100)
        
        # Configuración TCP para consulta al aggregator
        self.aggregator_host = config.get('aggregator_host', 'localhost')
        self.aggregator_port = config.get('aggregator_port', 50000)
        
        # Cliente TCP para comunicaciones con aggregator
        #self.tcp_client = TCPClient(self.aggregator_host, self.aggregator_port)
        
        # Estado
        self.processed_messages = set()
        self.batch_processed_counts = defaultdict(int)
        self.messages_since_last_checkpoint = 0
        self.messages_since_last_log = 0
        
        # Conexiones
        self.aggregator_producer = Producer(self.aggregator_queue)
        self.aggregator_consumer = Subscriber(self.aggregator_response_queue, 
                                             message_handler=self._handle_aggregator_response)
        
        # Crear directorio de persistencia
        os.makedirs(self.batch_persistence_dir, exist_ok=True)
        
        # Registrar callbacks para respuestas TCP
        self._register_tcp_callbacks()
    
    def _register_tcp_callbacks(self):
        """Registra callbacks para respuestas TCP del aggregator"""
        # Los callbacks ahora se registran temporalmente en cada consulta
        self.logger.info("Callbacks TCP se registran temporalmente en cada consulta")
    
    def start(self):
        """Inicia el consumer del aggregator"""
        self.aggregator_consumer.start()
        self.logger.info("Consumer del aggregator iniciado")
    
    def close(self):
        """Cierra conexiones de forma segura"""
        try:
            if hasattr(self, 'aggregator_consumer') and self.aggregator_consumer:
                self.aggregator_consumer.close()
        except Exception as e:
            self.logger.error(f"Error cerrando consumer: {e}")
        
        try:
            if hasattr(self, 'aggregator_producer') and self.aggregator_producer:
                self.aggregator_producer.close()
        except Exception as e:
            self.logger.error(f"Error cerrando producer: {e}")
        
        try:
            if hasattr(self, 'tcp_client') and self.tcp_client:
                self.tcp_client.close()
        except Exception as e:
            self.logger.error(f"Error cerrando TCP client: {e}")
    
    def load_checkpoint_and_recover(self, state_data):
        """Carga checkpoint y hace recuperación"""
        # 1. Cargar checkpoint
        self._load_checkpoint(state_data)
        
        # 2. Recuperar desde archivos de batch
        self._recover_from_batch_files(state_data['process_message'])
        
    
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
    
    def _recover_from_batch_files(self, process_message_callback):
        """Recupera estado consultando archivos de batch únicos"""
        if not os.path.exists(self.batch_persistence_dir):
            self.logger.info("No hay archivos de batch para recuperar")
            return
        
        self.logger.info("Iniciando recuperación desde archivos de batch...")
        
        batch_files = [f for f in os.listdir(self.batch_persistence_dir) 
                      if f.endswith('_batches.json')]
        
        self.logger.info(f"Encontrados {len(batch_files)} archivos de batch: {batch_files}")
        
        for batch_file in batch_files:
            try:
                client_id = batch_file.replace('_batches.json', '')
                self.logger.info(f"Procesando archivo de cliente {client_id}")
                
                with open(os.path.join(self.batch_persistence_dir, batch_file), "r") as f:
                    batches_data = json.load(f)
                
                # Procesar cada batch en el archivo
                for batch_id, batch_info in batches_data["batches"].items():
                    message_id = f"{client_id}_{batch_id}"
                    
                    # Verificar si está en el checkpoint
                    if message_id in self.processed_messages:
                        self.logger.info(f"Cliente {client_id}: Batch {batch_id} ya está en checkpoint")
                        continue
                    
                    # Si está marcado como completado pero no en checkpoint, agregarlo
                    if batch_info["status"] == "completed":
                        self.logger.info(f"Cliente {client_id}: Batch {batch_id} completado pero no en checkpoint, agregando")
                        self.processed_messages.add(message_id)
                        process_message_callback(batch_info["message"])
                    else:
                        # Batch no completado, consultar al aggregator
                        self.logger.info(f"Cliente {client_id}: Batch {batch_id} sin completar, consultando al aggregator...")
                        self._query_aggregator_for_batch_status(client_id, batch_id, batch_info, process_message_callback)
                        
            except Exception as e:
                self.logger.error(f"Error procesando archivo {batch_file}: {e}")
    
    def _query_aggregator_for_batch_status(self, client_id, batch_id, batch_info, process_message_callback):
        """Consulta al aggregator sobre el estado de un batch específico"""
        try:
            query_message = {
                "type": "batch_status_query",
                "client_id": client_id,
                "batch_id": batch_id,
                "joiner_id": self.joiner_id,
                "requesting_joiner": self.joiner_id
            }
            
            # Variable para almacenar la respuesta
            response_received = {"result": None, "received": False}
            
            # Callback temporal para capturar la respuesta
            def response_callback(response_data):
                response_received["result"] = response_data
                response_received["received"] = True
            
            # # Registrar callback temporal
            # self.tcp_client.register_response_callback("batch_status_response", response_callback)
            #
            # # Enviar consulta via TCP
            # if self.tcp_client.send(json.dumps(query_message) + '\n'):
            #     self.logger.info(f"Consulta enviada al aggregator para {client_id}_{batch_id}")
            #
            #     # Esperar respuesta con timeout
            #     timeout = 5  # 5 segundos de timeout
            #     start_time = time.time()
            #
            #     while not response_received["received"] and (time.time() - start_time) < timeout:
            #         time.sleep(0.1)  # Esperar 100ms entre checks
            #
            #     if response_received["received"]:
            #         response_data = response_received["result"]
            #         response_type = response_data.get("response_type")
            #         message_id = f"{client_id}_{batch_id}"
            #
            #         self.logger.info(f"Respuesta del aggregator para {message_id}: {response_type}")
            #
            #         if response_type == "batch_processed_by_me":
            #             self.logger.info(f"Batch {message_id} fue procesado por mí, agregando a processed_messages")
            #             self.processed_messages.add(message_id)
            #
            #             # Crear END_TRX ya que el aggregator confirma que lo procesé
            #             self._persist_end_transaction(client_id, batch_id)
            #
            #             # Procesar el mensaje localmente
            #             process_message_callback(batch_info["message"])
            #
            #         elif response_type == "batch_processed_by_other":
            #             self.logger.info(f"Batch {message_id} fue procesado por otro joiner, descartando")
            #             # Eliminar el archivo ya que fue procesado por otro
            #             self._remove_batch_file(client_id, batch_id)
            #
            #         elif response_type == "batch_not_processed":
            #             self.logger.info(f"Batch {message_id} no fue procesado, descartando archivo")
            #             # Eliminar el archivo ya que no fue procesado
            #             self._remove_batch_file(client_id, batch_id)
            #     else:
            #         self.logger.warning(f"Timeout esperando respuesta del aggregator para {client_id}_{batch_id}")
            #         # En caso de timeout, asumir que no fue procesado
            #         self._remove_batch_file(client_id, batch_id)
            # else:
            #     self.logger.error(f"No se pudo enviar consulta al aggregator para {client_id}_{batch_id}")
            #     # En caso de error, asumir que no fue procesado
            #     self._remove_batch_file(client_id, batch_id)
                
        except Exception as e:
            self.logger.error(f"Error consultando aggregator para {client_id}_{batch_id}: {e}")
            # En caso de excepción, asumir que no fue procesado
            self._remove_batch_file(client_id, batch_id)
    
    def _remove_batch_file(self, client_id, batch_id):
        """Elimina un batch específico del archivo de batches"""
        try:
            batch_file = os.path.join(self.batch_persistence_dir, f"{client_id}_batches.json")
            if os.path.exists(batch_file):
                with open(batch_file, "r") as f:
                    batches_data = json.load(f)
                
                # Eliminar el batch del archivo
                if batch_id in batches_data["batches"]:
                    del batches_data["batches"][batch_id]
                    
                    # Si no quedan batches, eliminar el archivo
                    if not batches_data["batches"]:
                        os.remove(batch_file)
                        self.logger.info(f"Archivo {batch_file} eliminado (sin batches)")
                    else:
                        # Guardar archivo actualizado
                        temp_file = batch_file + ".tmp"
                        with open(temp_file, "w") as f:
                            json.dump(batches_data, f, indent=2)
                        os.replace(temp_file, batch_file)
                        self.logger.info(f"Batch {batch_id} eliminado de {batch_file}")
                        
        except Exception as e:
            self.logger.error(f"Error eliminando batch {batch_id}: {e}")
    
    def _handle_aggregator_response(self, message):
        """Maneja respuestas del aggregator (para compatibilidad)"""
        # Este método se mantiene para compatibilidad con el sistema de colas
        # pero la recuperación principal ahora usa TCP
        pass
    
    def process_message(self, message, state_data, process_message_callback, send_ack_to_consumer):
        """Procesa un mensaje con recuperación y control"""
        client_id = message.get("client_id")
        batch_id = message.get("batch_id", "unknown")
        message_id = f"{client_id}_{batch_id}"
        
        # 1. Check O(1) si ya procesé localmente
        if message_id in self.processed_messages:
            self.logger.info(f"Mensaje ya procesado localmente: {message_id}")
            return False
        
        # # 2. Consultar al aggregator si ya fue procesado por otro joiner
        # if not self._check_with_aggregator_if_processed(client_id, batch_id):
        #     self.logger.info(f"Mensaje {message_id} ya fue procesado por otro joiner")
        #     return False
        
        try:
            # 3. Procesar el mensaje
            process_message_callback(message)
            
            # # 4. Persistir resultado
            # self._persist_batch_result(client_id, batch_id, message, state_data)
            
            # 5. Actualizar contadores
            self.batch_processed_counts[client_id] += 1
            
            # 6. Enviar ACK solo si el consumer está vivo
            try:
                send_ack_to_consumer(batch_id)
            except Exception as e:
                if "Channel is closed" in str(e):
                    self.logger.warning(f"Canal cerrado durante ACK, ignorando")

            
            # 7. Enviar control al aggregator
            self._send_batch_processed(client_id, batch_id, message)
            
            # 8. Marcar como procesado
            self.processed_messages.add(message_id)
            
            # # 9. Persistir END_TRX
            # self._persist_end_transaction(client_id, batch_id)
            
            # # 10. Logs y checkpoints
            # self._handle_periodic_operations(state_data)
            
            return True
            
        except Exception as e:
            self.logger.exception(f"Error procesando mensaje: {e}")
            return False 

    def _check_with_aggregator_if_processed(self, client_id, batch_id):
        """Consulta al aggregator si el mensaje ya fue procesado por otro joiner"""
        try:
            query_message = {
                "type": "message_processed_check",
                "client_id": client_id,
                "batch_id": batch_id,
                "joiner_id": self.joiner_id,
                "requesting_joiner": self.joiner_id
            }
            
            # Variable para almacenar la respuesta
            response_received = {"result": None, "received": False}
            
            # Callback temporal para capturar la respuesta
            def response_callback(response_data):
                response_received["result"] = response_data
                response_received["received"] = True
            
            # # Registrar callback temporal
            # self.tcp_client.register_response_callback("message_processed_response", response_callback)
            #
            # # Enviar consulta via TCP
            # if self.tcp_client.send(json.dumps(query_message) + '\n'):
            #     self.logger.info(f"Consulta enviada al aggregator para verificar {client_id}_{batch_id}")
            #
            #     # Esperar respuesta con timeout
            #     timeout = 5  # 5 segundos de timeout
            #     start_time = time.time()
            #
            #     while not response_received["received"] and (time.time() - start_time) < timeout:
            #         time.sleep(0.1)  # Esperar 100ms entre checks
            #
            #     if response_received["received"]:
            #         response_data = response_received["result"]
            #         was_processed = response_data.get("was_processed", False)
            #         processed_by = response_data.get("processed_by", None)
            #
            #         self.logger.info(f"Respuesta recibida: procesado={was_processed}, por={processed_by}")
            #
            #         if was_processed and processed_by != self.joiner_id:
            #             self.logger.info(f"Mensaje {client_id}_{batch_id} ya fue procesado por {processed_by}")
            #             return False  # No procesar
            #         else:
            #             self.logger.info(f"Mensaje {client_id}_{batch_id} no fue procesado o fue procesado por mí")
            #             return True  # Continuar procesamiento
            #     else:
            #         self.logger.warning(f"Timeout esperando respuesta del aggregator para {client_id}_{batch_id}")
            #         return True  # Continuar procesamiento en caso de timeout
            # else:
            #     self.logger.error(f"No se pudo enviar consulta de verificación al aggregator")
            #     return True  # Continuar procesamiento en caso de error
                
        except Exception as e:
            self.logger.error(f"Error consultando aggregator para verificación: {e}")
            return True  # Continuar procesamiento en caso de error
    
    def _persist_batch_result(self, client_id, batch_id, message, state_data):
        """Persiste resultado del batch en un único archivo por cliente"""
        batch_file = os.path.join(self.batch_persistence_dir, f"{client_id}_batches.json")
        
        # Cargar datos existentes o crear nuevo
        if os.path.exists(batch_file):
            with open(batch_file, "r") as f:
                batches_data = json.load(f)
        else:
            batches_data = {
                "client_id": client_id,
                "batches": {},
                "last_updated": time.time()
            }
        
        # Agregar el nuevo batch
        batches_data["batches"][batch_id] = {
            "timestamp": time.time(),
            "message": message,
            "status": "completed"
        }
        batches_data["last_updated"] = time.time()
        
        # Guardar de forma atómica
        temp_file = batch_file + ".tmp"
        with open(temp_file, "w") as f:
            json.dump(batches_data, f, indent=2)
        
        os.replace(temp_file, batch_file)
        self.logger.info(f"Batch {batch_id} agregado a {batch_file}")
    
    def _persist_end_transaction(self, client_id, batch_id):
        """Marca batch como completado en el archivo único"""
        batch_file = os.path.join(self.batch_persistence_dir, f"{client_id}_batches.json")
        
        if os.path.exists(batch_file):
            with open(batch_file, "r") as f:
                batches_data = json.load(f)
            
            # Marcar como completado
            if batch_id in batches_data["batches"]:
                batches_data["batches"][batch_id]["status"] = "completed"
                batches_data["last_updated"] = time.time()
                
                # Guardar de forma atómica
                temp_file = batch_file + ".tmp"
                with open(temp_file, "w") as f:
                    json.dump(batches_data, f, indent=2)
                
                os.replace(temp_file, batch_file)
                self.logger.info(f"Batch {batch_id} marcado como completado en {batch_file}")
    
    def _send_batch_processed(self, client_id, batch_id, message):
        """Envía mensaje de control al aggregator"""
        control_message = {
            "type": "control",
            "client_id": client_id,
            "batch_id": batch_id,
            # "processed_batches": self.batch_processed_counts[client_id],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "joiner_instance_id": self.joiner_id,
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
        
        # Checkpoint - REDUCIDO para guardar más frecuentemente
        self.messages_since_last_checkpoint += 1
        if self.messages_since_last_checkpoint >= self.checkpoint_interval:
            self._save_checkpoint(state_data)
            self.messages_since_last_checkpoint = 0
    
    def _log_current_state(self, state_data):
        """Log de control"""
        self.logger.info("=== ESTADO ACTUAL DEL JOINER ===")
        self.logger.info(f"Total de mensajes procesados: {len(self.processed_messages)}")
        
        # Agregar desglose por cliente
        client_counts = defaultdict(int)
        for message_id in self.processed_messages:
            client_id = message_id.split('_')[0]
            client_counts[client_id] += 1
        
        for client_id, count in client_counts.items():
            self.logger.info(f"Cliente {client_id}: {count} mensajes procesados")
        
        if 'log_state' in state_data:
            state_data['log_state']()
        
        self.logger.info("=== FIN ESTADO ACTUAL ===")
    
    def _save_checkpoint(self, state_data):
        """Guarda checkpoint y limpia archivos de transacciones procesadas"""
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
        
        # Limpiar archivos de transacciones procesadas
        self._cleanup_processed_transactions()
    
    def _cleanup_processed_transactions(self):
        """Limpia archivos de transacciones que ya están en el checkpoint"""
        if not os.path.exists(self.batch_persistence_dir):
            return
        
        self.logger.info("Limpiando archivos de transacciones procesadas...")
        cleaned_count = 0
        
        for filename in os.listdir(self.batch_persistence_dir):
            if filename.endswith('.json'):
                try:
                    if filename.endswith('_END_TRX.json'):
                        # Para archivos END_TRX, verificar si el batch correspondiente está procesado
                        batch_filename = filename.replace('_END_TRX.json', '.json')
                        parts = batch_filename.replace('.json', '').split('_')
                        if len(parts) >= 2:
                            batch_id = parts[-1]
                            client_id = '_'.join(parts[:-1])
                            message_id = f"{client_id}_{batch_id}"
                            
                            if message_id in self.processed_messages:
                                # Eliminar tanto el archivo de batch como el END_TRX
                                batch_file = os.path.join(self.batch_persistence_dir, batch_filename)
                                end_trx_file = os.path.join(self.batch_persistence_dir, filename)
                                
                                # Verificar que los archivos existen antes de eliminarlos
                                if os.path.exists(batch_file):
                                    os.remove(batch_file)
                                    cleaned_count += 1
                                    self.logger.debug(f"Archivo eliminado: {batch_filename}")
                                
                                if os.path.exists(end_trx_file):
                                    os.remove(end_trx_file)
                                    cleaned_count += 1
                                    self.logger.debug(f"Archivo eliminado: {filename}")
                                
                    else:
                        # Para archivos de batch normales, verificar si están procesados
                        parts = filename.replace('.json', '').split('_')
                        if len(parts) >= 2:
                            batch_id = parts[-1]
                            client_id = '_'.join(parts[:-1])
                            message_id = f"{client_id}_{batch_id}"
                            
                            if message_id in self.processed_messages:
                                file_path = os.path.join(self.batch_persistence_dir, filename)
                                if os.path.exists(file_path):
                                    os.remove(file_path)
                                    cleaned_count += 1
                                    self.logger.debug(f"Archivo eliminado: {filename}")
                                
                except Exception as e:
                    self.logger.error(f"Error limpiando archivo {filename}: {e}")
        
        self.logger.info(f"Limpieza completada: {cleaned_count} archivos eliminados")
    
    def save_final_checkpoint(self, state_data):
        """Guarda checkpoint final antes de cerrar"""
        if len(self.processed_messages) > 0:
            self.logger.info("Guardando checkpoint final antes de cerrar...")
            self._save_checkpoint(state_data) 
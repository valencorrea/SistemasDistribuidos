import json
import time
from typing import Dict, Any

class ServiceParser:
    @staticmethod
    def parse_heartbeat(data: str) -> Dict[str, Any]:
        """
        Parsea el mensaje de heartbeat recibido del servicio.
        
        Args:
            data (str): Mensaje JSON con la información del heartbeat
            
        Returns:
            Dict[str, Any]: Diccionario con la información parseada
        """
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in heartbeat message")
    
    @staticmethod
    def create_heartbeat(service_name: str) -> str:
        """
        Crea un mensaje de heartbeat para enviar al monitor.
        
        Args:
            service_name (str): Nombre del servicio que envía el heartbeat
            
        Returns:
            str: Mensaje JSON con la información del heartbeat
        """
        return json.dumps({
            "service_name": service_name,
            "timestamp": time.time() * 1000  # timestamp en milisegundos
        }) 
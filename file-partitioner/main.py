import pandas as pd
import os
import sys
sys.path.append('..')
from middleware.base_publisher import BasePublisher
import numpy as np

class FilePartitioner:
    def __init__(self, host='rabbitmq'):
        self.publisher = BasePublisher(host=host)
        self.setup_queues()
        
    def setup_queues(self):
        # Colas para distribución de trabajo
        self.publisher.setup_queue('partition_movies')
        self.publisher.setup_queue('partition_ratings')
        self.publisher.setup_queue('partition_credits')
        
    def partition_file(self, file_path, num_partitions=5):
        """Particiona un archivo CSV en chunks y los distribuye"""
        df = pd.read_csv(file_path)
        chunks = np.array_split(df, num_partitions)
        
        # Determinar el tipo de archivo
        if 'movies_metadata' in file_path:
            queue_name = 'partition_movies'
        elif 'ratings' in file_path:
            queue_name = 'partition_ratings'
        elif 'credits' in file_path:
            queue_name = 'partition_credits'
        else:
            raise ValueError(f"Tipo de archivo no soportado: {file_path}")
            
        # Distribuir los chunks
        for i, chunk in enumerate(chunks):
            message = {
                'partition_id': i,
                'total_partitions': num_partitions,
                'data': chunk.to_dict('records')
            }
            self.publisher.publish(queue_name, message, wait_response=False)
            
    def process_files(self):
        """Procesa todos los archivos en el directorio de datos"""
        data_dir = '/app/data'
        files = [
            'movies_metadata.csv',
            'ratings.csv',
            'credits.csv'
        ]
        
        for file in files:
            file_path = os.path.join(data_dir, file)
            if os.path.exists(file_path):
                print(f"Particionando archivo: {file}")
                self.partition_file(file_path)
            else:
                print(f"Archivo no encontrado: {file}")

if __name__ == '__main__':
    partitioner = FilePartitioner(host=os.getenv('RABBITMQ_HOST', 'rabbitmq'))
    partitioner.process_files() 
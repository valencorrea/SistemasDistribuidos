import os
import sys
sys.path.append('..')
from middleware.base_worker import BaseWorker
import pandas as pd

class PartitionWorker(BaseWorker):
    def __init__(self, queue_name, filter_type, host='rabbitmq'):
        super().__init__(queue_name, host)
        self.filter_type = filter_type
        self.data = []
        
    def callback(self, message):
        partition_id = message['partition_id']
        total_partitions = message['total_partitions']
        chunk_data = pd.DataFrame(message['data'])
        
        print(f"Procesando partición {partition_id + 1} de {total_partitions}")
        
        if self.filter_type == 'movies':
            filtered_data = self.filter_movies(chunk_data)
        elif self.filter_type == 'ratings':
            filtered_data = self.filter_ratings(chunk_data)
        elif self.filter_type == 'credits':
            filtered_data = self.filter_credits(chunk_data)
            
        self.data.extend(filtered_data.to_dict('records'))
        return None
        
    def filter_movies(self, df):
        """Aplica filtros específicos para películas"""
        # Ejemplo: películas después del 2000
        return df[df['release_date'].str[:4].astype(float) >= 2000]
        
    def filter_ratings(self, df):
        """Aplica filtros específicos para ratings"""
        # Ejemplo: ratings mayores a 3.5
        return df[df['rating'] > 3.5]
        
    def filter_credits(self, df):
        """Aplica filtros específicos para créditos"""
        # Ejemplo: actores principales
        return df[df['order'] <= 3]

def create_workers(num_workers=2):
    """Crea múltiples workers para cada tipo de datos"""
    worker_types = [
        ('partition_movies', 'movies'),
        ('partition_ratings', 'ratings'),
        ('partition_credits', 'credits')
    ]
    
    workers = []
    for queue_name, filter_type in worker_types:
        for _ in range(num_workers):
            worker = PartitionWorker(
                queue_name=queue_name,
                filter_type=filter_type,
                host=os.getenv('RABBITMQ_HOST', 'rabbitmq')
            )
            workers.append(worker)
    
    return workers

if __name__ == '__main__':
    workers = create_workers()
    
    # Iniciar todos los workers
    import multiprocessing
    processes = []
    
    for worker in workers:
        p = multiprocessing.Process(target=worker.start)
        p.start()
        processes.append(p)
        
    for p in processes:
        p.join() 
import os
import sys
sys.path.append('..')
from worker.base_worker import BaseWorker
import csv
import io

class MovieFilterWorker(BaseWorker):
    def __init__(self, filter_type: str, host: str = 'localhost', port: int = 5000):
        super().__init__(queue_name='movies_queue', host=host, port=port)
        self.filter_type = filter_type
        
    def process_chunk(self, chunk_data: list) -> list:
        filtered_data = []
        for row in chunk_data:
            if isinstance(row, str):
                row = next(csv.DictReader(io.StringIO(row)))
                
            if self.filter_type == 'post_2000':
                if self._is_post_2000(row):
                    filtered_data.append(row)
            elif self.filter_type == 'argentina_spain':
                if self._is_argentina_spain(row):
                    filtered_data.append(row)
                    
        return filtered_data
        
    def _is_post_2000(self, row: dict) -> bool:
        try:
            release_date = row.get('release_date', '')
            if release_date:
                year = int(release_date[:4])
                return year >= 2000
        except:
            pass
        return False
        
    def _is_argentina_spain(self, row: dict) -> bool:
        countries = row.get('production_countries', '').lower()
        return 'argentina' in countries or 'spain' in countries
        
    def get_target_queue(self) -> str:
        return 'aggregator_queue'

if __name__ == '__main__':
    import threading
    
    # Crear 3 workers para cada tipo de filtro
    workers = []
    for filter_type in ['post_2000', 'argentina_spain']:
        for _ in range(3):
            worker = MovieFilterWorker(filter_type=filter_type)
            workers.append(worker)
    
    # Iniciar todos los workers en threads separados
    threads = []
    for worker in workers:
        thread = threading.Thread(target=worker.start)
        thread.start()
        threads.append(thread)
        
    for thread in threads:
        thread.join() 
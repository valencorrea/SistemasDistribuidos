import os
import sys
sys.path.append('..')
from worker.base_worker import BaseWorker
from typing import Dict, List
import json

class ResultAggregator(BaseWorker):
    def __init__(self, host: str = 'localhost', port: int = 5000):
        super().__init__(queue_name='aggregator_queue', host=host, port=port)
        self.results: Dict[str, List] = {
            'post_2000': [],
            'argentina_spain': []
        }
        
    def process_chunk(self, chunk_data: list) -> list:
        for result in chunk_data:
            if 'release_date' in result:
                year = int(result['release_date'][:4])
                if year >= 2000:
                    if result not in self.results['post_2000']:
                        self.results['post_2000'].append(result)
                if 'argentina' in result.get('production_countries', '').lower() or \
                   'spain' in result.get('production_countries', '').lower():
                    if result not in self.results['argentina_spain']:
                        self.results['argentina_spain'].append(result)
                    
        if self.total_lines and self.processed_lines >= self.total_lines:
            self._save_results()
            
        return []
        
    def _save_results(self):
        os.makedirs('results', exist_ok=True)
        for result_type, data in self.results.items():
            output_file = f'results/{result_type}_results.json'
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Resultados guardados en {output_file}")
                
    def get_target_queue(self) -> str:
        return None

if __name__ == '__main__':
    aggregator = ResultAggregator()
    aggregator.start() 
from flask import Flask, jsonify
import pandas as pd
import os
import sys
sys.path.append('..')
from middleware.base_publisher import BasePublisher

app = Flask(__name__)

class MovieAggregator:
    def __init__(self, host='rabbitmq'):
        self.publisher = BasePublisher(host=host)
        self.setup_queues()
        
    def setup_queues(self):
        self.publisher.setup_queue('movie_requests')
        self.publisher.setup_queue('sentiment_requests')
        
    def get_argentina_spain_movies(self):
        return self.publisher.publish('movie_requests', {'type': 'argentina_spain'})
        
    def get_top_budget_countries(self):
        return self.publisher.publish('movie_requests', {'type': 'top_budget'})
        
    def get_sentiment_analysis(self):
        results = self.publisher.publish('sentiment_requests', {'type': 'analyze'})
        df = pd.DataFrame(results)
        sentiment_stats = df.groupby('sentiment').agg({
            'revenue': 'mean',
            'budget': 'mean'
        }).reset_index()
        return sentiment_stats.to_dict('records')

aggregator = MovieAggregator(host=os.getenv('RABBITMQ_HOST', 'rabbitmq'))

@app.route('/query/argentina-spain-2000', methods=['GET'])
def get_argentina_spain_movies():
    try:
        result = aggregator.get_argentina_spain_movies()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/query/top-budget-countries', methods=['GET'])
def get_top_budget_countries():
    try:
        result = aggregator.get_top_budget_countries()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/query/sentiment-analysis', methods=['GET'])
def get_sentiment_analysis():
    try:
        result = aggregator.get_sentiment_analysis()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) 
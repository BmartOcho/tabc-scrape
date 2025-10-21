"""
Web server for health monitoring and API endpoints
"""

import logging
from flask import Flask, jsonify
from .config import config
from .storage.database import DatabaseManager
from .data.api_client import TexasComptrollerAPI

logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/health')
def health_check():
    """Basic health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'environment': config.api.base_url,
        'timestamp': '2023-10-01T00:00:00Z'  # Placeholder
    })

@app.route('/status')
def system_status():
    """Detailed system status"""
    try:
        db_manager = DatabaseManager()
        api_client = TexasComptrollerAPI()

        db_ok = db_manager.test_connection()
        api_ok = api_client.test_connection()

        stats = db_manager.get_enrichment_stats() if db_ok else {}

        return jsonify({
            'database_connected': db_ok,
            'api_connected': api_ok,
            'enrichment_stats': stats,
            'config': config.to_dict()
        })
    except Exception as e:
        logger.error(f"Error in status endpoint: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/metrics')
def metrics():
    """Application metrics"""
    try:
        db_manager = DatabaseManager()
        stats = db_manager.get_enrichment_stats()

        return jsonify({
            'total_restaurants': stats.get('total_restaurants', 0),
            'enriched_restaurants': stats.get('restaurants_with_concept_classification', 0),
            'uptime': 'unknown'  # Placeholder
        })
    except Exception as e:
        logger.error(f"Error in metrics endpoint: {e}")
        return jsonify({'error': str(e)}), 500

def run_server(host='0.0.0.0', port=5000, debug=False):
    """Run the Flask server"""
    app.run(host=host, port=port, debug=debug)
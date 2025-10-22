"""
Web server for health monitoring and API endpoints
"""

import logging
from flask import Flask, jsonify, request, Response
from .config import config
from .storage.database import DatabaseManager
from .data.api_client import TexasComptrollerAPI
import pandas as pd
import io

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

@app.route('/api/enriched-data')
def get_enriched_data():
    """Get enriched restaurant data in JSON format"""
    try:
        logger.info("API request for enriched data (JSON)")
        db_manager = DatabaseManager()

        # Get limit from query parameter
        limit = request.args.get('limit', type=int)

        df = db_manager.get_enriched_restaurants_dataframe(limit=limit)
        if df.empty:
            logger.warning("No enriched data available")
            return jsonify({'error': 'No enriched data available'}), 404

        data = df.to_dict('records')
        logger.info(f"Returning {len(data)} enriched restaurant records")
        return jsonify(data)

    except Exception as e:
        logger.error(f"Error in enriched-data endpoint: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/enriched-data/csv')
def get_enriched_data_csv():
    """Get enriched restaurant data in CSV format"""
    try:
        logger.info("API request for enriched data (CSV)")
        db_manager = DatabaseManager()

        # Get limit from query parameter
        limit = request.args.get('limit', type=int)

        df = db_manager.get_enriched_restaurants_dataframe(limit=limit)
        if df.empty:
            logger.warning("No enriched data available for CSV")
            return Response("No enriched data available", status=404, mimetype='text/plain')

        # Convert to CSV
        output = io.StringIO()
        df.to_csv(output, index=False)
        csv_data = output.getvalue()
        output.close()

        logger.info(f"Returning CSV with {len(df)} enriched restaurant records")
        return Response(csv_data, mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=enriched_restaurants.csv'})

    except Exception as e:
        logger.error(f"Error in enriched-data CSV endpoint: {e}")
        return Response(f"Error: {str(e)}", status=500, mimetype='text/plain')

@app.route('/api/restaurants/<restaurant_id>')
def get_restaurant_by_id(restaurant_id):
    """Get specific restaurant data by ID"""
    try:
        logger.info(f"API request for restaurant {restaurant_id}")
        db_manager = DatabaseManager()

        restaurant = db_manager.get_restaurant_dict_by_id(restaurant_id)
        if not restaurant:
            logger.warning(f"Restaurant {restaurant_id} not found")
            return jsonify({'error': 'Restaurant not found'}), 404

        # Get enriched data for this restaurant
        df = db_manager.get_enriched_restaurants_dataframe(limit=None)
        enriched_row = df[df['id'] == restaurant_id]
        if not enriched_row.empty:
            restaurant = enriched_row.iloc[0].to_dict()

        logger.info(f"Returning data for restaurant {restaurant_id}")
        return jsonify(restaurant)

    except Exception as e:
        logger.error(f"Error in restaurant endpoint: {e}")
        return jsonify({'error': str(e)}), 500

def run_server(host='0.0.0.0', port=5000, debug=False):
    """Run the Flask server"""
    app.run(host=host, port=port, debug=debug)
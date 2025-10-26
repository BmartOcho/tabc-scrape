"""
Web server for health monitoring and API endpoints
"""

import asyncio
import logging
import time
import subprocess
import threading
import numpy as np
from flask import Flask, jsonify, request, Response
from .config import config
from .storage.database import DatabaseManager
from .data.api_client import TexasComptrollerAPI
from .workflow import WorkflowManager
import pandas as pd
import io
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST

logger = logging.getLogger(__name__)

# Prometheus metrics setup
registry = CollectorRegistry()
app = Flask(__name__)

# Define metrics
REQUEST_COUNT = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status_code'],
    registry=registry
)

REQUEST_DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint'],
    registry=registry
)

RESTAURANT_COUNT = Gauge(
    'restaurants_total',
    'Total number of restaurants in database',
    registry=registry
)

ENRICHED_RESTAURANT_COUNT = Gauge(
    'restaurants_enriched_total',
    'Total number of enriched restaurants',
    registry=registry
)

API_CALLS_TOTAL = Counter(
    'api_calls_total',
    'Total API calls made',
    ['api_name', 'status'],
    registry=registry
)

# Middleware to track HTTP requests
@app.before_request
def before_request():
    request.start_time = time.time()

@app.after_request
def after_request(response):
    if hasattr(request, 'start_time'):
        duration = time.time() - request.start_time
        REQUEST_DURATION.labels(
            method=request.method,
            endpoint=request.endpoint or 'unknown'
        ).observe(duration)

        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.endpoint or 'unknown',
            status_code=str(response.status_code)
        ).inc()

    return response

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
        api_ok = asyncio.run(api_client.test_connection())

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
    """Prometheus metrics endpoint"""
    try:
        # Update gauge metrics with current database stats
        db_manager = DatabaseManager()
        stats = db_manager.get_enrichment_stats()

        RESTAURANT_COUNT.set(stats.get('total_restaurants', 0))
        ENRICHED_RESTAURANT_COUNT.set(stats.get('restaurants_with_concept_classification', 0))

        # Generate and return Prometheus metrics
        return Response(generate_latest(registry), mimetype=CONTENT_TYPE_LATEST)
    except Exception as e:
        logger.error(f"Error in metrics endpoint: {e}")
        return Response(f"Error generating metrics: {str(e)}", status=500, mimetype='text/plain')

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

        # Replace NaN and inf values with None for valid JSON serialization
        df = df.replace([np.nan, np.inf, -np.inf], None)
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

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>TABC Restaurant Data Pipeline</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .card { border: 1px solid #ddd; padding: 20px; margin: 20px 0; border-radius: 8px; }
            .button {
                background: #007bff;
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                margin: 5px;
            }
            .button:hover { background: #0056b3; }
            .status { padding: 10px; margin: 10px 0; border-radius: 4px; }
            .success { background: #d4edda; color: #155724; }
            .error { background: #f8d7da; color: #721c24; }
            .info { background: #d1ecf1; color: #0c5460; }
        </style>
    </head>
    <body>
        <h1>🍽️ TABC Restaurant Data Pipeline</h1>
        
        <div class="card">
            <h2>System Status</h2>
            <div id="status">Loading...</div>
        </div>
        
        <div class="card">
            <h2>Data Collection</h2>
            <button class="button" onclick="fetchData()">Fetch Restaurant Data</button>
            <button class="button" onclick="enrichData()">Enrich Data</button>
            <button class="button" onclick="exportData()">Export Data</button>
        </div>
        
        <div class="card">
            <h2>Statistics</h2>
            <div id="stats">Loading...</div>
        </div>
        
        <div class="card">
            <h2>Recent Data</h2>
            <div id="recent-data">Loading...</div>
        </div>
        
        <script>
            async function loadStatus() {
                const response = await fetch('/status');
                const data = await response.json();
                
                const statusDiv = document.getElementById('status');
                statusDiv.innerHTML = `
                    <div class="status ${data.database_connected ? 'success' : 'error'}">
                        Database: ${data.database_connected ? '✓ Connected' : '✗ Disconnected'}
                    </div>
                    <div class="status ${data.api_connected ? 'success' : 'error'}">
                        API: ${data.api_connected ? '✓ Connected' : '✗ Disconnected'}
                    </div>
                `;
                
                const statsDiv = document.getElementById('stats');
                const stats = data.enrichment_stats;
                statsDiv.innerHTML = `
                    <p><strong>Total Restaurants:</strong> ${stats.total_restaurants || 0}</p>
                    <p><strong>With Concept Classification:</strong> ${stats.restaurants_with_concept_classification || 0}</p>
                    <p><strong>With Population Data:</strong> ${stats.restaurants_with_population_data || 0}</p>
                    <p><strong>With Square Footage:</strong> ${stats.restaurants_with_square_footage || 0}</p>
                `;
            }
            
            async function loadRecentData() {
                const response = await fetch('/api/enriched-data?limit=5');
                const data = await response.json();
                
                const dataDiv = document.getElementById('recent-data');
                if (Array.isArray(data) && data.length > 0) {
                    dataDiv.innerHTML = '<table style="width:100%; border-collapse: collapse;">' +
                        '<tr style="border-bottom: 2px solid #ddd;">' +
                        '<th>Name</th><th>City</th><th>Concept</th><th>Receipts</th></tr>' +
                        data.map(r => `
                            <tr style="border-bottom: 1px solid #ddd;">
                                <td>${r.location_name || 'N/A'}</td>
                                <td>${r.location_city || 'N/A'}</td>
                                <td>${r.concept_primary || 'N/A'}</td>
                                <td>$${(r.total_receipts || 0).toLocaleString()}</td>
                            </tr>
                        `).join('') +
                        '</table>';
                } else {
                    dataDiv.innerHTML = '<p>No data available yet. Fetch some data to get started!</p>';
                }
            }
            
            async function fetchData() {
                alert('Fetching data... This may take a few minutes.');
                const response = await fetch('/api/trigger/fetch', { method: 'POST', body: JSON.stringify({ limit: 100 }), headers: { 'Content-Type': 'application/json' } });
                const result = await response.json();
                alert(result.message || result.error);
                window.location.reload();
            }
            
            async function enrichData() {
                alert('Enriching data... This may take several minutes.');
                const response = await fetch('/api/trigger/enrich', { method: 'POST', body: JSON.stringify({ limit: 10 }), headers: { 'Content-Type': 'application/json' } });
                const result = await response.json();
                alert(result.message || result.error);
                window.location.reload();
            }
            
            async function exportData() {
                window.location.href = '/api/enriched-data/csv';
            }
            
            // Load data on page load
            loadStatus();
            loadRecentData();
            
            // Refresh every 30 seconds
            setInterval(() => {
                loadStatus();
                loadRecentData();
            }, 30000);
        </script>
    </body>
    </html>
    '''

@app.route('/api/trigger/fetch', methods=['POST'])
def trigger_fetch():
    """Trigger data fetch operation"""
    try:
        limit = request.json.get('limit', 100)
        
        # Run fetch command in background
        def run_fetch():
            subprocess.run([
                'python', '-m', 'src.tabc_scrape.cli',
                'fetch', '--limit', str(limit)
            ], check=True)
        
        thread = threading.Thread(target=run_fetch, daemon=True)
        thread.start()
        
        return jsonify({'status': 'started', 'message': 'Fetch operation started'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/trigger/enrich', methods=['POST'])
def trigger_enrich():
    """Trigger data enrichment operation"""
    try:
        limit = request.json.get('limit', 10)
        
        # Run enrich command in background
        def run_enrich():
            subprocess.run([
                'python', '-m', 'src.tabc_scrape.cli',
                'enrich', '--limit', str(limit)
            ], check=True)
        
        thread = threading.Thread(target=run_enrich, daemon=True)
        thread.start()
        
        return jsonify({'status': 'started', 'message': 'Enrichment operation started'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/workflow/full', methods=['POST'])
def run_full_workflow():
    """Run the complete pipeline"""
    try:
        limit = request.json.get('limit', 50)
        
        workflow = WorkflowManager()
        
        # Run in background
        def run_async():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(workflow.run_full_pipeline(limit))
            logger.info(f"Workflow completed: {result}")
        
        thread = threading.Thread(target=run_async, daemon=True)
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': f'Full pipeline started with limit={limit}'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def run_server(host='0.0.0.0', port=5000, debug=False):
    """Run the Flask server"""
    app.run(host=host, port=port, debug=debug)
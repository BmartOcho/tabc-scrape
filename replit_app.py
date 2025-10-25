import asyncio
import threading
import os
from src.tabc_scrape.web import app, run_server

def start_web_server():
    """Start the web server in a separate thread"""
    run_server(host='0.0.0.0', port=5000, debug=False)

if __name__ == '__main__':
    # Start web server in background
    server_thread = threading.Thread(target=start_web_server, daemon=True)
    server_thread.start()
    
    # Keep main thread alive
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Shutting down...")
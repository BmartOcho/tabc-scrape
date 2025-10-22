import os
import sys

# Set environment variables for this session
os.environ['API_KEY_ID'] = '1qah3gxsa7hq22uv7pq1qb6j2'
os.environ['API_KEY_SECRET'] = 'owqiugc9q94bnvh5r11n3ibvhq1zs2k72lnz8re98woh0neug'
os.environ['APP_TOKEN'] = '2d7MaXhf0SeDpdQ1gEmJ80Jjy'

# Add src to path
sys.path.append('src')

print("=== DEBUG TEST ===", flush=True)
print(f"Current directory: {os.getcwd()}", flush=True)
print(f"Python path: {sys.path}", flush=True)

print("=== ENVIRONMENT VARIABLES ===", flush=True)
print(f'API_KEY_ID: {os.getenv("API_KEY_ID")}', flush=True)
print(f'API_KEY_SECRET: {os.getenv("API_KEY_SECRET")}', flush=True)
print(f'APP_TOKEN: {os.getenv("APP_TOKEN")}', flush=True)

print("=== TESTING IMPORTS ===", flush=True)
try:
    from tabc_scrape.config import config
    print("Config imported successfully", flush=True)
    print(f'Config API Key ID: {config.api.api_key_id[:10] + "..." if config.api.api_key_id else "None"}', flush=True)
    print(f'Config API Key Secret: {"Set" if config.api.api_key_secret else "None"}', flush=True)
    print(f'Config App Token: {config.api.app_token[:10] + "..." if config.api.app_token else "None"}', flush=True)
    print(f'Config Base URL: {config.api.base_url}', flush=True)
except Exception as e:
    print(f'Error importing config: {e}', flush=True)
    import traceback
    traceback.print_exc()

print("=== TEST COMPLETE ===", flush=True)
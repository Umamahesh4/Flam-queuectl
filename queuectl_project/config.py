# Create this file: queuectl_project/config.py
import os
import json

CONFIG_DIR = os.path.expanduser('~/.queuectl')
CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')
DB_PATH = os.path.join(CONFIG_DIR, 'jobs.db')
PID_FILE = os.path.join(CONFIG_DIR, 'workers.pid')

DEFAULT_CONFIG = {
    'max_retries': 3,
    'backoff_base': 2
}

def load_config() -> dict:
    os.makedirs(CONFIG_DIR, exist_ok=True)
    if not os.path.exists(CONFIG_FILE):
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG

def save_config(config_data: dict):
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config_data, f, indent=2)
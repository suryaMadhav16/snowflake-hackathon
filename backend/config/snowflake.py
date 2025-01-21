import os
from pathlib import Path
from typing import Dict, Optional

def load_snowflake_config() -> Dict[str, str]:
    """Load Snowflake configuration from environment or config file"""
    
    # Try to load from environment variables first
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'MEDIUM'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'LLM'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAG')
    }
    
    # Check if all required values are present
    required_keys = ['account', 'user', 'password']
    missing_keys = [key for key in required_keys if not config[key]]
    
    if missing_keys:
        # Try to load from config file
        config_path = Path(__file__).parent / 'snowflake.conf'
        if config_path.exists():
            with open(config_path) as f:
                for line in f:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        config[key.strip()] = value.strip()
    
    return config

def validate_config(config: Dict[str, str]) -> Optional[str]:
    """Validate Snowflake configuration"""
    required_keys = ['account', 'user', 'password']
    missing_keys = [key for key in required_keys if not config.get(key)]
    
    if missing_keys:
        return f"Missing required Snowflake configuration: {', '.join(missing_keys)}"
    return None
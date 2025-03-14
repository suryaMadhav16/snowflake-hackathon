"""Entry point for the FastAPI backend server.

This module is responsible for starting the FastAPI application using Uvicorn.
It sets up the necessary environment for the application to run, including:
1. Adding the backend directory to the Python path for module imports.
2. Configuring the Uvicorn server to run the FastAPI app defined in main.py.

To run the server, execute this script directly.

Example:
    To start the server:
        $ python run.py

This will start the server on http://0.0.0.0:8000 with auto-reload enabled.
"""

import os
import sys
import uvicorn

# Add the backend directory to Python path
backend_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(backend_dir, 'src')
sys.path.insert(0, src_dir)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[backend_dir]
    )

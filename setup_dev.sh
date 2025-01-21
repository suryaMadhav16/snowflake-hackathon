#!/bin/bash

# Create virtual environments
echo "Setting up backend environment..."
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install pytest pytest-cov pytest-asyncio
deactivate

echo "Setting up frontend environment..."
cd ../frontend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate

echo "Setup complete!"

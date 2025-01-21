#!/bin/bash

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run tests with pytest
pytest tests/ -v --cov=app --cov-report=term-missing

# If you want to generate HTML coverage report
# pytest tests/ -v --cov=app --cov-report=html

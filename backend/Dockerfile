# Base stage for shared dependencies
FROM python:3.11-slim AS base

WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .
EXPOSE 8000

# Development stage
FROM base AS development
# Install development dependencies
RUN pip install pytest pytest-cov pylint black isort

# Use run.py with reload for development
CMD ["python", "run.py"]

# Production stage
FROM base AS production
# Install production-specific dependencies
RUN pip install gunicorn

# Copy application code
COPY . .
EXPOSE 8000

# Change this line to directly use run.py with environment variable
ENV RELOAD=False
CMD ["python", "run.py"]
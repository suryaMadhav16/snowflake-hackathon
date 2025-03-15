# Base stage for shared dependencies
FROM python:3.11-slim AS base

WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Modified pip installation command for Snowflake packages
RUN pip install --extra-index-url https://repo.anaconda.com/pkgs/snowflake -r requirements.txt

# Development stage
FROM base AS development
# Additional development dependencies if needed
RUN pip install watchdog

# Copy application code
COPY . .
EXPOSE 8501

# Use development configuration
ENV STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_RUN_ON_SAVE=true

CMD ["streamlit", "run", "main.py"]

# Production stage
FROM base AS production
# Copy application code
COPY . .
EXPOSE 8501

# Use production configuration
ENV STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_RUN_ON_SAVE=false

CMD ["streamlit", "run", "main.py"]
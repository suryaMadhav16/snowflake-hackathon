#!/bin/bash

# Configuration
GCP_INSTANCE="your-instance-name"
GCP_ZONE="your-zone"
GCP_PROJECT="your-project-id"
REMOTE_USER="crawler_user"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Setting up GCP instance for the first time...${NC}"

# SSH into instance and setup
gcloud compute ssh $REMOTE_USER@$GCP_INSTANCE --zone=$GCP_ZONE --project=$GCP_PROJECT << 'EOF'
    # Create directory structure
    sudo mkdir -p /opt/crawler/{backend,venv}
    sudo chown -R $USER:$USER /opt/crawler

    # Setup Python virtual environment
    sudo apt-get update
    sudo apt-get install -y python3-venv python3-pip

    # Create virtual environment
    python3 -m venv /opt/crawler/venv
    
    # Install basic requirements
    source /opt/crawler/venv/bin/activate
    pip install fastapi uvicorn

    echo -e "${GREEN}Basic setup complete!${NC}"
EOF
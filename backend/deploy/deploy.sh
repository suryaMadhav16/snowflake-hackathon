#!/bin/bash

# Configuration
GCP_INSTANCE="your-instance-name"
GCP_ZONE="your-zone"
GCP_PROJECT="your-project-id"
REMOTE_USER="crawler_user"
DEPLOY_PATH="/opt/crawler"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Starting deployment...${NC}"

# Create deployment package
echo "Creating deployment package..."
cd ..
tar --exclude='*.pyc' --exclude='__pycache__' --exclude='.git' --exclude='deploy' -czf deploy/deploy.tar.gz .

# Copy files to instance
echo "Copying files to instance..."
gcloud compute scp deploy/deploy.tar.gz $REMOTE_USER@$GCP_INSTANCE:$DEPLOY_PATH/deploy.tar.gz --zone=$GCP_ZONE --project=$GCP_PROJECT

# Remote deployment
gcloud compute ssh $REMOTE_USER@$GCP_INSTANCE --zone=$GCP_ZONE --project=$GCP_PROJECT << 'EOF'
    # Stop the service
    sudo systemctl stop crawler-backend

    # Clean old files
    cd /opt/crawler
    rm -rf backend/*

    # Extract new files
    cd backend
    tar xzf ../deploy.tar.gz

    # Update virtual environment
    source ../venv/bin/activate
    pip install -r requirements.txt

    # Copy service file if doesn't exist
    if [ ! -f "/etc/systemd/system/crawler-backend.service" ]; then
        sudo cp deploy/crawler-backend.service /etc/systemd/system/
        sudo systemctl daemon-reload
    fi

    # Restart service
    sudo systemctl start crawler-backend
    sudo systemctl enable crawler-backend

    # Clean up
    cd ..
    rm deploy.tar.gz

    # Check status
    sleep 2
    sudo systemctl status crawler-backend
EOF

echo -e "${GREEN}Deployment complete!${NC}"
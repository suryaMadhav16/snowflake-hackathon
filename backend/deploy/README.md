# Web Crawler Backend Deployment Guide

This guide explains how to deploy the web crawler backend to Google Cloud Platform (GCP) Compute Engine.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [First-Time Setup](#first-time-setup)
3. [Regular Deployment](#regular-deployment)
4. [Environment Variables](#environment-variables)
5. [Service Management](#service-management)
6. [Troubleshooting](#troubleshooting)

## Prerequisites

1. Local Machine:
   - Google Cloud SDK installed
   - gcloud CLI configured with your project
   - Python 3.7+ installed

2. GCP Project:
   - Compute Engine API enabled
   - Service account with proper permissions
   - Compute Engine instance created

3. Instance Requirements:
   - Ubuntu 20.04+ recommended
   - At least 2GB RAM
   - Python 3.7+ installed

## First-Time Setup

1. Configure Deployment Scripts

Edit `setup.sh` and `deploy.sh` to set your GCP details:
```bash
GCP_INSTANCE="your-instance-name"  # e.g., crawler-backend
GCP_ZONE="your-zone"              # e.g., us-central1-a
GCP_PROJECT="your-project-id"     # e.g., my-crawler-project
REMOTE_USER="crawler_user"        # your GCP instance username
```

2. Run First-Time Setup
```bash
cd backend/deploy
chmod +x setup.sh deploy.sh
./setup.sh
```

This will:
- Create necessary directories on the instance
- Set up Python virtual environment
- Install basic dependencies
- Configure systemd service

## Regular Deployment

To deploy your latest code:
```bash
cd backend/deploy
./deploy.sh
```

The deployment script:
1. Creates a deployment package (deploy.tar.gz)
2. Uploads it to your GCP instance
3. Extracts files and updates dependencies
4. Restarts the service
5. Shows service status

## Environment Variables

1. Create .env file in backend/deploy:
```bash
# .env
DATABASE_URL=sqlite:///path/to/db.sqlite
LOG_LEVEL=INFO
MAX_WORKERS=4
```

2. Variables are automatically deployed with your code

To add new variables:
1. Add them to your .env file
2. Deploy normally using deploy.sh

To modify existing variables:
1. Update .env file
2. Run deploy.sh

## Service Management

The backend runs as a systemd service. Common commands:

```bash
# Check status
sudo systemctl status crawler-backend

# Stop service
sudo systemctl stop crawler-backend

# Start service
sudo systemctl start crawler-backend

# View logs
sudo journalctl -u crawler-backend
```

Service Configuration:
- Service file: /etc/systemd/system/crawler-backend.service
- Working directory: /opt/crawler/backend
- Virtual environment: /opt/crawler/venv
- Log output: systemd journal

## Troubleshooting

1. Deployment Fails
   - Check GCP instance connectivity
   - Verify permissions
   - Check disk space
   ```bash
   df -h /opt/crawler
   ```

2. Service Won't Start
   - Check logs
   ```bash
   sudo journalctl -u crawler-backend -n 100
   ```
   - Verify environment variables
   - Check Python dependencies

3. Memory Issues
   - Check memory usage
   ```bash
   free -h
   ```
   - Monitor process
   ```bash
   top -u crawler_user
   ```

4. Permission Issues
   - Check file ownership
   ```bash
   ls -la /opt/crawler
   ```
   - Fix permissions if needed
   ```bash
   sudo chown -R crawler_user:crawler_user /opt/crawler
   ```

## Directory Structure

```
/opt/crawler/
├── backend/                # Application code
│   ├── main.py
│   ├── requirements.txt
│   └── .env
├── venv/                  # Python virtual environment
└── deploy.tar.gz          # Latest deployment package
```

## Useful Commands

1. Check Application Status:
```bash
curl http://localhost:8000/health
```

2. View Real-time Logs:
```bash
sudo journalctl -u crawler-backend -f
```

3. Check Resource Usage:
```bash
htop -u crawler_user
```

4. Backup Database:
```bash
cp /opt/crawler/backend/crawler.db /opt/crawler/backups/
```

## Security Notes

1. Firewall Rules:
   - Only expose necessary ports
   - Use GCP firewall rules for access control

2. Environment Variables:
   - Never commit .env files
   - Use secrets management for production

3. File Permissions:
   - Keep strict permissions on .env
   - Run service as non-root user

## Maintenance

1. Regular Tasks:
   - Monitor disk space
   - Check logs for errors
   - Update dependencies

2. Backup Strategy:
   - Regular database backups
   - Keep deployment packages
   - Document configuration changes

## Tips

1. Development:
   - Test locally before deploying
   - Use similar Python version
   - Match production configs

2. Deployment:
   - Deploy during low-traffic periods
   - Keep backup of working deployment
   - Monitor after deployment

3. Performance:
   - Monitor memory usage
   - Watch CPU utilization
   - Check disk I/O

## Support

For issues:
1. Check service logs
2. Review deployment logs
3. Verify configurations
4. Check instance resources

Contact repository maintainers for additional help.
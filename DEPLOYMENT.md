# Deployment Guide

This guide explains three different ways to deploy the application:
1. Using docker-compose for full stack deployment
2. Deploying only the backend
3. Deploying only the frontend

## Prerequisites

- Docker installed and running
- Make utility installed
- Git (for cloning the repository)
- Python 3.10

## 1. Full Stack Deployment Using Docker Compose

### Environment Setup
1. Create environment files for both services:

   For backend (`backend/.env`):
   ```env
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ROLE=your_role
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   ```

   For frontend (`frontend/.env`):
   ```env
   BACKEND_URL=http://backend:8000  # Uses Docker service name
   DEBUG=true  # Set to false in prod
   ```

2. Ensure Streamlit secrets are configured in `frontend/.streamlit/secrets.toml`

### Available Commands

#### Development Environment
```bash
# Start all services in development mode
make dev

# View logs
make dev-logs              # All services
make dev-backend-logs      # Only backend
make dev-frontend-logs     # Only frontend

# Access service shells
make dev-backend-shell
make dev-frontend-shell

# Stop all services
make dev-down

# Check service status
make status
```

#### Production Environment
```bash
# Start all services in production mode
make prod

# View logs
make prod-logs
make prod-backend-logs
make prod-frontend-logs

# Access service shells
make prod-backend-shell
make prod-frontend-shell

# Stop all services
make prod-down
```

#### Utility Commands
```bash
# Remove all containers, networks, and images
make clean

# View all available commands
make help
```

### Service URLs
- Backend: http://localhost:8000
- Frontend: http://localhost:8501

## 2. Backend-Only Deployment

The backend can be deployed independently using its own Makefile in the backend directory.

### Available Commands

```bash
cd backend

# Development
make dev              # Build and start development server
make dev-logs         # View development logs
make dev-shell        # Access development container shell
make dev-stop         # Stop development container

# Production
make prod            # Build and start production server
make prod-logs       # View production logs
make prod-shell      # Access production container shell
make prod-stop       # Stop production container

# Utilities
make status          # View container status
make clean           # Remove containers and images
```

### Advanced Usage

#### Container Management
The stop commands (dev-stop/prod-stop) by default both stop and remove containers. To only stop without removing:

```bash
# Stop development container without removing
docker stop snowflake-backend-dev

# Stop production container without removing
docker stop snowflake-backend-prod

# Later, to remove stopped containers
docker rm snowflake-backend-dev
docker rm snowflake-backend-prod
```

#### Rebuilding
To rebuild containers with updated code:
```bash
make rebuild-dev     # Rebuild development container
make rebuild-prod    # Rebuild production container
```

## 3. Frontend-Only Deployment

The frontend can be deployed independently using its own Makefile in the frontend directory.

### Available Commands

```bash
cd frontend

# Development
make dev              # Build and start development server
make dev-logs         # View development logs
make dev-shell        # Access development container shell
make dev-stop         # Stop development container

# Production
make prod            # Build and start production server
make prod-logs       # View production logs
make prod-shell      # Access production container shell
make prod-stop       # Stop production container

# Utilities
make status          # View container status
make clean           # Remove containers and images
```

### Advanced Usage

#### Container Management
Similar to backend, to manage containers without removing:

```bash
# Stop development container without removing
docker stop snowflake-frontend-dev

# Stop production container without removing
docker stop snowflake-frontend-prod

# Later, to remove stopped containers
docker rm snowflake-frontend-dev
docker rm snowflake-frontend-prod
```

#### Volume Mounts
The frontend deployment includes special volume mounts:
- Development: Mounts entire app directory and .streamlit for live updates
- Production: Mounts only .streamlit directory for secrets

### Port Usage
- Development: 8501
- Production: 8502 (to avoid conflicts when running both environments)

## Important Notes

1. Environment Variables:
   - Use `.env.dev` for development
   - Use `.env.prod` for production
   - Never commit these files to version control

2. Secrets Management:
   - Keep Streamlit secrets in `.streamlit/secrets.toml`
   - Never commit secrets to version control
   - Consider using a secrets management service in production

3. Health Checks:
   - Backend has health checks enabled
   - Frontend waits for backend to be healthy before starting

4. Logging:
   - All containers output logs to stdout/stderr
   - Use the respective logging commands to view logs
   - Logs are not persisted by default

5. Network:
   - Services use Docker networks for communication
   - Development and production use separate networks
   - Backend is accessible by service name in docker-compose

## Troubleshooting

1. Container Issues:
```bash
# View container logs
docker logs <container_name>

# Check container status
docker ps -a

# Inspect container
docker inspect <container_name>
```

2. Network Issues:
```bash
# List networks
docker network ls

# Inspect network
docker network inspect <network_name>
```

3. Common Problems:
   - Port conflicts: Check if ports 8000, 8501, or 8502 are already in use
   - Environment variables: Verify all required variables are set
   - Permissions: Check volume mount permissions
   - Health checks: Verify backend health endpoint is responding

4. Reset Everything:
```bash
# From project root
make clean
docker system prune -f  # Be careful with this command
```

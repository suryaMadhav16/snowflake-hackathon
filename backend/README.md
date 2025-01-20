# Web Crawler Backend

FastAPI-based backend service for the web crawler application with Snowflake integration.

## Features

- FastAPI-based RESTful API and WebSocket endpoints
- Snowflake integration for data storage
- Real-time crawling updates via WebSocket
- Task-based crawling system
- File storage in Snowflake Stage
- Dockerized deployment

## Project Structure

```
backend/
├── app/
│   ├── api/
│   │   ├── routes.py       # API endpoints
│   │   └── schemas.py      # Pydantic models
│   ├── core/
│   │   ├── config.py           # Application settings
│   │   ├── task_manager.py     # Crawling task management
│   │   ├── storage_manager.py  # File storage in Snowflake
│   │   └── websocket_manager.py # WebSocket handling
│   ├── database/
│   │   └── snowflake_manager.py # Snowflake operations
│   └── main.py            # Application entry point
├── Dockerfile            # Docker configuration
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables:
```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=LLM
SNOWFLAKE_SCHEMA=RAG
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

3. Run the application:
```bash
uvicorn app.main:app --reload
```

## API Documentation

Once running, visit:
- Swagger UI: http://localhost:8000/api/v1/docs
- ReDoc: http://localhost:8000/api/v1/redoc

## Key Endpoints

### REST API

- `POST /api/v1/discover` - Discover URLs from target site
- `POST /api/v1/crawl` - Start crawling process
- `GET /api/v1/status/{task_id}` - Get crawling status
- `GET /api/v1/results/{url}` - Get crawling results
- `GET /api/v1/files/{url}` - Get saved files
- `GET /api/v1/stats` - Get crawler statistics

### WebSocket

- `/api/v1/ws/metrics/{task_id}` - Stream metrics updates
- `/api/v1/ws/progress/{task_id}` - Stream progress updates

## Docker Deployment

1. Build image:
```bash
docker build -t crawler-backend .
```

2. Run container:
```bash
docker run -p 8000:8000 \
  -e SNOWFLAKE_ACCOUNT=your_account \
  -e SNOWFLAKE_USER=your_user \
  -e SNOWFLAKE_PASSWORD=your_password \
  crawler-backend
```

## Development

1. Testing:
```bash
pytest tests/
```

2. Code formatting:
```bash
black app/
isort app/
```

3. Linting:
```bash
flake8 app/
mypy app/
```

## Architecture

The backend follows a layered architecture:

1. **API Layer** (app/api)
   - REST endpoints
   - WebSocket handlers
   - Request/response models

2. **Core Layer** (app/core)
   - Task management
   - File storage
   - WebSocket management
   - Configuration

3. **Database Layer** (app/database)
   - Snowflake connection
   - Query execution
   - Data models

## Contributing

1. Fork the repository
2. Create your feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

MIT License
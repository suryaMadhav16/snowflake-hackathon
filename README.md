# Web Crawler & RAG Chat Application

This application consists of a FastAPI backend for web crawling and a Streamlit frontend for user interaction and visualization.

## Prerequisites

- Python 3.11 or higher
- pip (Python package installer)
- Git

## Project Structure

```
.
├── backend/           # FastAPI backend server
├── frontend/         # Streamlit frontend application
├── requirements.txt  # Global requirements
└── README.md        # This file
```

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd snowflake-hackathon
```

### 2. Set Up Backend

```bash
# Create and activate virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate

# Navigate to backend directory
cd backend

# Install backend dependencies
pip install -r requirements.txt

# Create .env file from example (if provided)
cp config/snowflake.conf.example config/snowflake.conf  # Modify with your Snowflake credentials
```

### 3. Set Up Frontend

```bash
# Open a new terminal
# Create and activate virtual environment (optional but recommended)
python -m venv venv_frontend
source venv_frontend/bin/activate  # On Windows, use: venv_frontend\Scripts\activate

# Navigate to frontend directory
cd frontend

# Install frontend dependencies
pip install -r requirements.txt
```

## Running the Application

### 1. Start the Backend Server

```bash
# In the backend directory
cd backend
python main.py
```

The backend server will start at `http://localhost:8000`
- API documentation: `http://localhost:8000/docs`
- ReDoc documentation: `http://localhost:8000/redoc`

### 2. Start the Frontend Application

```bash
# In the frontend directory (in a new terminal)
cd frontend
streamlit run main.py
```

The Streamlit frontend will start at `http://localhost:8501`

## Environment Variables

### Backend (.env file in backend directory)
Required environment variables for the backend:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

### Frontend (.env file in frontend directory)
Required environment variables for the frontend:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

## Application Flow

1. Start both backend and frontend servers using the instructions above
2. Access the frontend at `http://localhost:8501`
3. Use the Crawler page to discover and crawl web pages
4. Use the Chat page to interact with the crawled content

## Troubleshooting

1. Port Conflicts
   - If port 8000 is in use, modify the backend port in `backend/main.py`
   - If port 8501 is in use, start Streamlit with a different port:
     ```bash
     streamlit run frontend/main.py --server.port 8502
     ```

2. Connection Issues
   - Verify Snowflake credentials in your .env files
   - Check if both backend and frontend servers are running
   - Ensure all dependencies are installed correctly

3. Common Solutions
   - Clear browser cache
   - Restart both servers
   - Check log

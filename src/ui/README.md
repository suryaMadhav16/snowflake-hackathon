# UI Module Documentation

This module provides a Streamlit-based user interface for the web crawler application.

## Directory Structure

```
ui/
├── __init__.py
├── main_app.py      - Main application setup
├── state.py         - State management
└── components/      - UI components
    ├── __init__.py
    ├── monitor.py   - Progress monitoring
    ├── results.py   - Results display
    ├── settings.py  - Configuration UI
    └── url_tree.py  - URL visualization
```

## Core Components

### 1. Main Application (main_app.py)

#### CrawlerApp
Main application class that orchestrates the UI components and crawler operations.

**Methods:**
```python
async def discover_urls(self, url: str, settings: Dict) -> List[str]
```
- Handles URL discovery phase
- Returns list of discovered URLs

```python
async def process_urls(self, urls: List[str], settings: Dict)
```
- Processes discovered URLs in batches
- Updates progress and displays results

```python
async def run(self)
```
- Main application flow
- Renders UI components and handles user interaction

### 2. State Management (state.py)

#### CrawlerState
Dataclass representing crawler state.

#### StateManager
Manages application state in Streamlit's session state.

**Key Methods:**
```python
def update_progress(self, processed_urls: List[str], current_batch: int, total_batches: int)
```
- Updates crawling progress
- Manages state transitions

```python
def save_settings(self, settings: Dict)
```
- Saves crawler configuration
- Maintains settings persistence

## UI Components

### 1. Monitor Component (components/monitor.py)

#### CrawlerMonitor
Handles real-time progress monitoring and metrics display.

**Key Features:**
- Progress bar visualization
- Performance metrics charts
- Error monitoring
- Resource usage tracking

**Methods:**
```python
def render_progress(self, total_urls: int, processed_urls: int, current_metrics: Dict)
```
- Displays crawling progress
- Shows performance metrics

```python
def update_metrics(self, metrics: Dict)
```
- Updates metrics history
- Maintains performance tracking

### 2. Results Component (components/results.py)

#### ResultsDisplay
Manages display and analysis of crawling results.

**Key Features:**
- Results summary visualization
- Domain-based filtering
- Error analysis
- Performance trends

**Methods:**
```python
def render_results_summary(self, results: List[CrawlResult])
```
- Displays crawling results
- Shows success/failure metrics

```python
def render_error_analysis(self, results: List[CrawlResult])
```
- Analyzes and displays crawling errors
- Provides error patterns insight

### 3. Settings Component (components/settings.py)

#### Functions

```python
def render_crawler_settings() -> Dict[str, Any]
```
- Renders configuration UI
- Returns complete crawler settings

```python
def get_performance_settings(mode: str) -> Dict[str, Any]
```
- Returns performance presets
- Configures resource usage

**Key Features:**
- Browser configuration
- Performance tuning
- Anti-bot settings
- Media handling options

### 4. URL Tree Component (components/url_tree.py)

#### URLTreeVisualizer
Visualizes discovered URLs in a hierarchical structure.

**Key Features:**
- Tree visualization
- Domain grouping
- Path analysis
- Statistics display

**Methods:**
```python
def render_url_tree(self, urls: List[str], graph_data: Dict = None)
```
- Creates hierarchical URL visualization
- Shows domain relationships

## Key Features

1. **Real-time Monitoring**
   - Progress tracking
   - Performance metrics
   - Resource usage monitoring
   - Error tracking

2. **Results Analysis**
   - Success/failure metrics
   - Domain-based filtering
   - Error pattern analysis
   - Performance trends

3. **Configuration Management**
   - Performance modes
   - Browser settings
   - Anti-bot protection
   - Media handling

4. **URL Visualization**
   - Hierarchical structure
   - Domain grouping
   - Path analysis
   - Statistics

## Usage

The UI can be accessed by running:
```python
from ui.main_app import main

if __name__ == "__main__":
    main()
```

## Dependencies

- streamlit - Web interface framework
- plotly - Data visualization
- pandas - Data analysis
- asyncio - Async operations
- crawl4ai - Web crawling framework

## Implementation Details

1. **State Management**
   - Session state persistence
   - Configuration management
   - Progress tracking
   - Error logging

2. **Performance Optimization**
   - Batch processing
   - Resource monitoring
   - Configurable settings
   - Memory management

3. **User Experience**
   - Real-time updates
   - Interactive visualizations
   - Error reporting
   - Configuration guidance

4. **Data Visualization**
   - Progress charts
   - Performance metrics
   - URL structure
   - Error patterns

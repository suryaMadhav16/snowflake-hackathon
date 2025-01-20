# UI Module Documentation

This module provides a Streamlit-based user interface for the web crawler application.

## Directory Structure

```
ui/
├── __init__.py
├── main_app.py      - Main application setup
├── state.py         - State management
├── pages/          - Streamlit pages
└── components/      - UI components
```

## Core Components

### 1. Main Application (main_app.py)

The main application file that sets up the Streamlit interface and handles the core application flow.

### 2. State Management (state.py)

Manages application state using Streamlit's session state functionality.

### 3. Pages Directory

Contains individual Streamlit pages for different sections of the application.

### 4. Components Directory

Contains reusable UI components used across different pages.

## Key Features

1. **Multi-page Navigation**
   - Multiple pages for different functionalities
   - Clear navigation structure
   - Modular component design

2. **State Management**
   - Session state persistence
   - Configuration management
   - Progress tracking

3. **User Interface**
   - Intuitive layout
   - Interactive elements
   - Real-time updates
   - Error handling

## Usage

The UI can be accessed by running:
```python
streamlit run streamlit_app.py
```

## Dependencies

- streamlit - Web interface framework
- pandas - Data analysis
- asyncio - Async operations

## Implementation Notes

1. **State Management**
   - Uses Streamlit's session state
   - Persists configuration
   - Tracks application flow

2. **Page Organization**
   - Modular page structure
   - Clear navigation
   - Consistent layout

3. **Component Design**
   - Reusable components
   - Consistent styling
   - Error handling

4. **User Experience**
   - Intuitive workflow
   - Clear feedback
   - Error reporting

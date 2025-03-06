# Conversation Analytics Framework

A comprehensive framework for collecting, processing, and analyzing conversation data from NocoDB and storing it in MongoDB and S3 (as Parquet files) for efficient analytics.

## Features

- **Data Collection**: Fetch conversation and message data from NocoDB API
- **Data Processing**: Transform raw data into analytics-ready format
- **Categorization**: Automatically categorize conversations by topic, intent, and sentiment
- **Analytics**: Generate daily, weekly, and monthly analytics reports
- **Multi-threading**: Parallel processing for improved performance
- **GPU Acceleration**: Optional GPU support for computationally intensive tasks
- **Resumable Processing**: Track processing state for reliable incremental processing
- **MongoDB Storage**: Flexible document storage for operational use
- **S3 Parquet Storage**: Efficient columnar storage for analytics

## Installation

### Prerequisites

- Python 3.8+
- MongoDB (optional)
- AWS S3 access (optional)
- CUDA-compatible GPU (optional)

### Setup

1. Clone the repository:

```bash
git clone https://github.com/yourusername/conversation-analytics.git
cd conversation-analytics
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file based on the provided `.env.example`:

```bash
cp .env.example .env
```

4. Edit the `.env` file with your configuration settings:

```
# NocoDB API Configuration
NOCODB_BASE_URL=https://your-nocodb-instance.com
NOCODB_API_TOKEN=your-api-token
NOCODB_PROJECT_ID=your-project-id

# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=conversation_analytics

# S3 Configuration (optional)
S3_ENABLED=true
S3_BUCKET=your-analytics-bucket
S3_PREFIX=conversation-analytics
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
```

## Usage

### Command-Line Interface

The framework provides a command-line interface for easy use:

```bash
# Collect and process conversation data
python analytics_cli.py collect --start-date 2025-01-01 --end-date 2025-03-01

# Filter conversations by app ID
python analytics_cli.py collect --app-id your-app-id

# Resume from last processed conversation
python analytics_cli.py collect --resume

# Show processing status
python analytics_cli.py status
```

### Options

- `--start-date`: Start date for data collection (YYYY-MM-DD)
- `--end-date`: End date for data collection (YYYY-MM-DD)
- `--app-id`: App ID for filtering conversations
- `--batch-size`: Number of conversations to process in each batch (default: 100)
- `--no-mongodb`: Skip MongoDB storage
- `--resume`: Resume from last processed conversation
- `--state-file`: Path to the processing state file (default: processing_state.json)
- `--use-s3-state`: Use S3 for state tracking

### Python API

You can also use the framework programmatically:

```python
from analytics_framework.main import main

# Run with default settings
main()
```

## Architecture

The framework follows a modular architecture:

```
analytics_framework/
  ├── api/
  │   └── nocodb_client.py      # NocoDB API client
  ├── models/
  │   └── mongodb_schema.py     # MongoDB schema definitions
  ├── processors/
  │   └── data_processor.py     # Data processing logic
  ├── storage/
  │   └── mongodb_client.py     # MongoDB client
  ├── utils/
  │   ├── processing_state.py   # Processing state tracking
  │   └── thread_pool.py        # Multi-threading support
  ├── config.py                 # Configuration settings
  └── main.py                   # Main entry point
```

## Data Model

### MongoDB Collections

1. **conversation_analytics**: Merged conversation data with analytics fields
2. **conversation_messages**: Individual messages within conversations
3. **conversation_categories**: Categorization data for conversations
4. **conversation_translations**: Translation data for conversations/messages
5. **analytics_reports**: Pre-computed analytics reports
6. **user_analytics**: User-specific analytics data

### S3 Parquet Structure

```
s3://bucket/
  ├── conversations/
  │   ├── year=2025/
  │   │   ├── month=03/
  │   │   │   ├── day=01/
  │   │   │   │   ├── conversations.parquet
  │   │   │   │   ├── messages/
  │   │   │   │   │   └── messages.parquet
  │   │   │   │   └── categories/
  │   │   │   │       └── categories.parquet
  │   │   │   └── day=02/
  │   │   │       └── ...
  │   │   └── month=04/
  │   │       └── ...
  │   └── year=2024/
  │       └── ...
  ├── user_analytics/
  │   └── user_analytics.parquet
  └── analytics_reports/
      ├── daily/
      │   └── ...
      ├── weekly/
      │   └── ...
      └── monthly/
          └── ...
```

## Performance Optimization

### Multi-threading

The framework uses multi-threading to parallelize data processing:

- **IO Threads**: For network and disk operations
- **Processing Threads**: For CPU-intensive tasks
- **Process Workers**: For truly parallel processing

Configure thread counts in the `.env` file:

```
ENABLE_MULTITHREADING=true
MAX_WORKERS=8
IO_THREADS=4
PROCESSING_THREADS=4
```

### GPU Acceleration

For computationally intensive tasks like categorization, the framework can use GPU acceleration:

```
ENABLE_GPU=true
GPU_MEMORY_LIMIT=4096  # MB
CUDA_VISIBLE_DEVICES=0  # Comma-separated list of GPU indices to use
```

## Development

### Git Hooks

This project uses Git hooks to automate certain tasks:

#### Pre-commit Hook

A pre-commit hook is included that automatically syncs your requirements.txt file with uv whenever you make changes to Python files or dependency-related files. This ensures that your requirements.txt file is always up-to-date with your actual dependencies.

To install the pre-commit hook:

**Windows (PowerShell):**

```powershell
.\install-hooks.ps1
```

**macOS/Linux/Git Bash:**

```bash
./install-hooks.sh
```

The pre-commit hook will:

1. Check if any Python files or dependency files have changed
2. If changes are detected, use uv to export the current dependencies
3. Update requirements.txt while preserving comments and structure
4. Add the updated requirements.txt to the git staging area

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. Make sure to install the pre-commit hook to keep the requirements.txt file in sync.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

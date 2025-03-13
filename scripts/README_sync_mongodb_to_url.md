# MongoDB to URL Sync Script

This script syncs conversation data from MongoDB to a specified URL using POST requests. It supports incremental syncing by tracking the last synced conversation.

## Features

- Retrieves conversations from MongoDB
- Sends data to a specified URL using POST requests
- Supports incremental syncing by tracking the last synced conversation
- Parallel processing for improved performance
- Configurable batch size, retry logic, and more
- Comprehensive error handling and logging

## Usage

```bash
python scripts/sync_mongodb_to_url.py URL [options]
```

### Arguments

- `URL`: Required. The URL to send data to.

### Options

- `--days DAYS`: Number of days ago to start syncing from
- `--start-time START_TIME`: Start time for syncing (ISO format)
- `--limit LIMIT`: Maximum number of conversations to sync
- `--batch-size BATCH_SIZE`: Number of conversations to process in each batch (default: from config)
- `--force-full-sync`: Force a full sync ignoring last sync state
- `--state-file STATE_FILE`: File to store sync state (default: sync_url_state.json)
- `--header HEADER`: HTTP header in the format 'key:value' (can be used multiple times)
- `--workers WORKERS`: Number of worker threads for parallel requests (default: 8)
- `--sequential`: Use sequential requests instead of parallel

## Examples

### Basic Usage

```bash
python scripts/sync_mongodb_to_url.py https://api.example.com/conversations
```

### With Authentication Header

```bash
python scripts/sync_mongodb_to_url.py http://localhost:8080/api/v2/tables/mhqfo42zs27ynnd/records --header "xc-token: AAO9I26nJ2MI_nJVMb6gZsbSHt3F0FP-fM5m6b_y" --force-full-sync
```

### Sync Last 30 Days

```bash
python scripts/sync_mongodb_to_url.py https://api.example.com/conversations --days 30
```

### Force Full Sync

```bash
python scripts/sync_mongodb_to_url.py https://api.example.com/conversations --force-full-sync
```

### Limit Number of Conversations

```bash
python scripts/sync_mongodb_to_url.py https://api.example.com/conversations --limit 1000
```

## Configuration

The script uses the following configuration from the analytics framework:

- `MONGODB_URI`: MongoDB connection URI
- `MONGODB_DATABASE`: MongoDB database name
- `MONGODB_CONVERSATIONS_COLLECTION`: MongoDB collection name for conversations
- `MAX_RETRIES`: Maximum number of retries for failed requests
- `RETRY_DELAY`: Delay between retries in seconds
- `IO_THREADS`: Base number of worker threads for parallel requests
- `BATCH_SIZE`: Number of conversations to process in each batch

By default, the script uses twice the number of IO_THREADS for parallel requests to maximize throughput. You can adjust this with the `--workers` option.

These can be configured in the `.env` file.

## State Management

The script maintains a state file (default: `sync_url_state.json`) to track the last synced conversation. This allows for incremental syncing, where only new or updated conversations are processed in subsequent runs.

To force a full sync, use the `--force-full-sync` option.

# Technical Context: Conversation Analytics Framework

## Technology Stack

The Conversation Analytics Framework is built using a modern technology stack designed for performance, scalability, and maintainability.

### Core Technologies

| Technology | Purpose | Version |
|------------|---------|---------|
| Python | Primary programming language | 3.8+ |
| MongoDB | Operational database for flexible document storage | 4.11.2 |
| S3 | Object storage for Parquet files | N/A |
| Parquet | Columnar file format for analytics | N/A |
| NocoDB | Data source for conversation data | N/A |

### Key Libraries and Dependencies

#### Core Dependencies

- **requests (2.32.3)**: HTTP client for API communication
- **pymongo (4.11.2)**: MongoDB client for Python
- **python-dotenv (1.0.1)**: Environment variable management

#### Data Processing

- **pandas (2.2.3)**: Data manipulation and analysis
- **pyarrow (19.0.1)**: Apache Arrow implementation for Python
- **fastparquet (2024.11.0)**: Parquet file format implementation

#### AWS Integration

- **boto3 (1.37.1)**: AWS SDK for Python
- **s3fs (2025.2.0)**: Filesystem interface to S3

#### Parallel Processing

- **joblib (1.4.2)**: Parallel computing utilities
- **dask (2025.2.0)**: Parallel computing framework
- **distributed (2025.2.0)**: Distributed computing

#### Utilities

- **tqdm (4.67.1)**: Progress bar for long-running operations
- **tabulate (0.9.0)**: Pretty-print tabular data

#### Optional GPU Acceleration

- **cudf**: GPU-accelerated DataFrame library (commented out)
- **cuml**: GPU-accelerated machine learning (commented out)

## Development Environment

### Prerequisites

- Python 3.8+
- MongoDB (optional)
- AWS S3 access (optional)
- CUDA-compatible GPU (optional)

### Setup Process

1. Clone the repository
2. Install dependencies with `pip install -r requirements.txt`
3. Create a `.env` file based on `.env.example`
4. Configure environment variables

### Development Tools

- **Git**: Version control
- **Pre-commit hooks**: Automated requirements.txt synchronization
- **uv**: Python package manager for dependency management

## Configuration

The framework is configured through environment variables, which can be set in a `.env` file or directly in the environment.

### Key Configuration Categories

#### NocoDB API Configuration

- `NOCODB_BASE_URL`: Base URL of the NocoDB instance
- `NOCODB_API_TOKEN`: API token for authentication
- `NOCODB_PROJECT_ID`: Project ID in NocoDB
- `NOCODB_CONVERSATION_TABLE`: Table name for conversations
- `NOCODB_MESSAGES_TABLE`: Table name for messages

#### MongoDB Configuration

- `MONGODB_URI`: Connection URI for MongoDB
- `MONGODB_DATABASE`: Database name
- `MONGODB_CONVERSATIONS_COLLECTION`: Collection name for conversations
- `MONGODB_TRANSLATIONS_COLLECTION`: Collection name for translations
- `MONGODB_ANALYTICS_REPORTS_COLLECTION`: Collection name for analytics reports
- `MONGODB_USER_ANALYTICS_COLLECTION`: Collection name for user analytics

#### S3 Storage Configuration

- `S3_ENABLED`: Whether S3 storage is enabled
- `S3_BUCKET`: S3 bucket name
- `S3_PREFIX`: Prefix for S3 objects
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_REGION`: AWS region

#### Parquet Optimization

- `PARQUET_STORAGE_ENABLED`: Whether Parquet storage is enabled
- `PARQUET_BASE_DIR`: Base directory for local Parquet files
- `PARQUET_PARTITION_BY`: Partitioning strategy
- `PARQUET_COMPRESSION`: Compression algorithm
- `PARQUET_ROW_GROUP_SIZE`: Row group size
- `PARQUET_PAGE_SIZE`: Page size
- `PARQUET_TARGET_FILE_SIZE_MB`: Target file size
- `PARQUET_MAX_RECORDS_PER_FILE`: Maximum records per file

#### Multi-Threading Configuration

- `ENABLE_MULTITHREADING`: Whether multi-threading is enabled
- `MAX_WORKERS`: Maximum number of worker threads
- `CHUNK_SIZE`: Size of data chunks for processing
- `IO_THREADS`: Number of I/O threads
- `PROCESSING_THREADS`: Number of processing threads

#### GPU Configuration

- `ENABLE_GPU`: Whether GPU acceleration is enabled
- `GPU_MEMORY_LIMIT`: GPU memory limit
- `CUDA_VISIBLE_DEVICES`: GPU devices to use

#### Batch Processing Configuration

- `BATCH_SIZE`: Batch size for processing
- `MAX_RETRIES`: Maximum number of retries
- `RETRY_DELAY`: Delay between retries

#### Logging Configuration

- `LOG_LEVEL`: Logging level
- `LOG_FILE`: Log file path

## Technical Constraints

### Performance Constraints

- Processing large volumes of conversation data requires efficient algorithms and parallel processing
- MongoDB query performance depends on proper indexing and query optimization
- S3 access can be a bottleneck for large-scale analytics

### Scalability Constraints

- MongoDB scaling requires proper sharding and indexing strategies
- S3 has eventual consistency, which can affect real-time analytics
- Multi-threading is limited by available CPU cores and memory

### Security Constraints

- API tokens and AWS credentials must be securely managed
- Data privacy considerations for conversation content
- Access control for analytics data

### Compatibility Constraints

- Python version compatibility (3.8+)
- MongoDB version compatibility (4.x+)
- AWS SDK version compatibility

## Technical Decisions

### MongoDB vs. SQL Database

MongoDB was chosen for its flexible schema, which is well-suited for conversation data that may have varying structures. It also provides good performance for document-oriented queries and supports rich indexing.

### Parquet vs. CSV for Analytics

Parquet was selected for its columnar storage format, which provides significant performance benefits for analytical queries. It also offers efficient compression and encoding schemes, reducing storage costs.

### Multi-threading vs. Multi-processing

Multi-threading was chosen for its lower overhead and shared memory model, which is beneficial for data processing tasks that involve large datasets. The GIL (Global Interpreter Lock) is not a significant bottleneck for I/O-bound operations.

### Local vs. S3 Storage

The framework supports both local and S3 storage, with S3 being the preferred option for production environments due to its scalability, durability, and integration with analytics tools. Local storage is useful for development and testing.

### Synchronous vs. Asynchronous Processing

The framework primarily uses synchronous processing for simplicity and reliability. Asynchronous processing could be considered for future enhancements, particularly for I/O-bound operations.

## External API Integration

### Dify Workflow API

#### Authentication

The Dify Service API uses API-Key authentication. API Keys should be stored securely on the server-side, not on the client-side, to prevent API-Key leakage.

For all API requests, include the API Key in the Authorization HTTP header:

```
Authorization: Bearer {API_KEY}
```

#### Workflow Execution

**Endpoint**: `POST /workflows/run`

**Purpose**: Execute a published workflow

**Request Body Parameters**:

- `inputs` (object, required): Contains key/value pairs for workflow variables. At least one key/value pair is required. For file-type variables, specify an object with the file description keys.
- `response_mode` (string, required): Response return mode
  - `streaming`: Streaming mode (recommended) using SSE (Server-Sent Events)
  - `blocking`: Blocking mode, returns result after completion (may time out after 100s due to Cloudflare restrictions)
- `user` (string, required): Unique identifier to identify the end-user for retrieval and statistics
- `files` (array[object], optional): File list for models that support file parsing
  - `type` (string): Supported file types
    - `document`: TXT, MD, MARKDOWN, PDF, HTML, XLSX, XLS, DOCX, CSV, EML, MSG, PPTX, PPT, XML, EPUB
    - `image`: JPG, JPEG, PNG, GIF, WEBP, SVG
    - `audio`: MP3, M4A, WAV, WEBM, AMR
    - `video`: MP4, MOV, MPEG, MPGA
    - `custom`: Other file types
  - `transfer_method` (string): `remote_url` for image URL / `local_file` for file upload
  - `url` (string): Image URL (when transfer_method is remote_url)
  - `upload_file_id` (string): Uploaded file ID from File Upload API (when transfer_method is local_file)

#### Response Types

**Blocking Mode Response** (CompletionResponse):

Content-Type: application/json

- `workflow_run_id` (string): Unique ID of workflow execution
- `task_id` (string): Task ID for request tracking
- `data` (object): Result details
  - `id` (string): ID of workflow execution
  - `workflow_id` (string): ID of related workflow
  - `status` (string): Execution status (running/succeeded/failed/stopped)
  - `outputs` (json, optional): Content output
  - `error` (string, optional): Error reason
  - `elapsed_time` (float, optional): Total seconds used
  - `total_tokens` (int, optional): Tokens used
  - `total_steps` (int): Default 0
  - `created_at` (timestamp): Start time
  - `finished_at` (timestamp): End time

**Streaming Mode Response** (ChunkCompletionResponse):

Content-Type: text/event-stream

Each streaming chunk starts with `data:` followed by the content, separated by `\n\n`. Example:

```
data: {"event": "message", "task_id": "900bbd43-dc0b-4383-a372-aa6e6c414227", "id": "663c5084-a254-4040-8ad3-51f2a3c1a77c", "answer": "Hi", "created_at": 1705398420}\n\n
```

Event types:

- `workflow_started`: Workflow starts execution
- `node_started`: Node execution started
- `node_finished`: Node execution ends
- `workflow_finished`: Workflow execution ends
- `tts_message`: TTS audio stream event (base64 encoded audio)
- `tts_message_end`: TTS audio stream end event
- `ping`: Ping event every 10s to keep connection alive

#### Error Codes

- 400, `invalid_param`: Abnormal parameter input
- 400, `app_unavailable`: App configuration unavailable
- 400, `provider_not_initialize`: No available model credential configuration
- 400, `provider_quota_exceeded`: Model invocation quota insufficient
- 400, `model_currently_not_support`: Current model unavailable
- 400, `workflow_request_error`: Workflow execution failed
- 500, Internal server error

#### Implementation Example

```python
import requests
import json

def execute_dify_workflow(api_key, inputs=None, response_mode="blocking", user_id="user_123"):
    """
    Execute a Dify workflow
    
    Args:
        api_key (str): Dify API key
        inputs (dict): Workflow inputs
        response_mode (str): Response mode (streaming or blocking)
        user_id (str): Unique user identifier
        
    Returns:
        dict: Response from Dify API
    """
    url = "https://aitool.liveperson88.com/v1/workflows/run"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "inputs": inputs or {},
        "response_mode": response_mode,
        "user": user_id
    }
    
    if response_mode == "blocking":
        response = requests.post(url, headers=headers, json=data)
        return response.json()
    else:  # streaming mode
        response = requests.post(url, headers=headers, json(data), stream=True)
        for line in response.iter_lines():
            if line:
                line = line.decode('utf-8')
                if line.startswith('data:'):
                    data = json.loads(line[5:])
                    # Process streaming data here
                    yield data
```

## Integration Points

### NocoDB API

- **Endpoint**: `{NOCODB_BASE_URL}/api/v1/db/data/v1/{NOCODB_PROJECT_ID}/{NOCODB_CONVERSATION_TABLE}`
- **Authentication**: Bearer token
- **Data Format**: JSON

### MongoDB

- **Connection**: `{MONGODB_URI}/{MONGODB_DATABASE}`
- **Collections**: Conversations, messages, categories, translations, reports, user analytics
- **Authentication**: Username/password or connection string

### S3

- **Bucket**: `{S3_BUCKET}`
- **Prefix**: `{S3_PREFIX}`
- **Authentication**: AWS access key and secret key
- **Data Format**: Parquet files

## Deployment Considerations

### Environment Setup

- Python environment with required dependencies
- MongoDB instance (local or cloud-based)
- AWS S3 bucket with appropriate permissions
- Environment variables configured

### Resource Requirements

- CPU: Multi-core processor recommended for parallel processing
- Memory: Depends on data volume, minimum 4GB recommended
- Storage: Depends on data volume and retention policy
- Network: Reliable connection to NocoDB, MongoDB, and S3

### Monitoring and Logging

- Logging to file and/or console
- Processing state tracking
- Error reporting and alerting
- Performance metrics collection

### Scaling Strategies

- Horizontal scaling with multiple worker instances
- Vertical scaling with more powerful hardware
- Distributed processing with Dask
- GPU acceleration for computationally intensive tasks

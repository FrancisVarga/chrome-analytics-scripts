"""Configuration settings for the analytics data collection."""

import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# NocoDB API Configuration
NOCODB_BASE_URL = os.getenv("NOCODB_BASE_URL")
NOCODB_API_TOKEN = os.getenv("NOCODB_API_TOKEN")
NOCODB_PROJECT_ID = os.getenv("NOCODB_PROJECT_ID")

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "conversation_analytics")

# Table/Collection Names
NOCODB_CONVERSATION_TABLE = os.getenv("NOCODB_CONVERSATION_TABLE", "Conversation")
NOCODB_MESSAGES_TABLE = os.getenv("NOCODB_MESSAGES_TABLE", "Messages")

# MongoDB Collections
MONGODB_CONVERSATIONS_COLLECTION = os.getenv("MONGODB_CONVERSATIONS_COLLECTION", "conversations")
MONGODB_MESSAGES_COLLECTION = os.getenv("MONGODB_MESSAGES_COLLECTION", "messages")
MONGODB_CATEGORIES_COLLECTION = os.getenv("MONGODB_CATEGORIES_COLLECTION", "categories")
MONGODB_TRANSLATIONS_COLLECTION = os.getenv("MONGODB_TRANSLATIONS_COLLECTION", "translations")
MONGODB_ANALYTICS_REPORTS_COLLECTION = os.getenv("MONGODB_ANALYTICS_REPORTS_COLLECTION", "analytics_reports")
MONGODB_USER_ANALYTICS_COLLECTION = os.getenv("MONGODB_USER_ANALYTICS_COLLECTION", "user_analytics")

# Dify API Configuration
DIFY_API_KEY = os.getenv("DIFY_API_KEY")
DIFY_BASE_URL = os.getenv("DIFY_BASE_URL", "https://aitool.liveperson88.com/v1")
DIFY_WORKFLOW_ENABLED = os.getenv("DIFY_WORKFLOW_ENABLED", "false").lower() == "true"

# S3 Storage Configuration
S3_ENABLED = os.getenv("S3_ENABLED", "false").lower() == "true"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "conversation-analytics")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Parquet Optimization
PARQUET_STORAGE_ENABLED = os.getenv("PARQUET_STORAGE_ENABLED", "true").lower() == "true"
PARQUET_BASE_DIR = os.getenv("PARQUET_BASE_DIR", "./data/parquet")
PARQUET_PARTITION_BY = os.getenv("PARQUET_PARTITION_BY", "year,month,day").split(",")
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")
PARQUET_ROW_GROUP_SIZE = int(os.getenv("PARQUET_ROW_GROUP_SIZE", "100000"))
PARQUET_PAGE_SIZE = int(os.getenv("PARQUET_PAGE_SIZE", "8192"))
PARQUET_TARGET_FILE_SIZE_MB = int(os.getenv("PARQUET_TARGET_FILE_SIZE_MB", "128"))
PARQUET_MAX_RECORDS_PER_FILE = int(os.getenv("PARQUET_MAX_RECORDS_PER_FILE", "50000"))

# Multi-Threading Configuration
ENABLE_MULTITHREADING = os.getenv("ENABLE_MULTITHREADING", "true").lower() == "true"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", str(os.cpu_count() or 4)))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))
IO_THREADS = int(os.getenv("IO_THREADS", "4"))
PROCESSING_THREADS = int(os.getenv("PROCESSING_THREADS", "4"))

# GPU Configuration
ENABLE_GPU = os.getenv("ENABLE_GPU", "false").lower() == "true"
try:
    # Try to parse the GPU_MEMORY_LIMIT as an integer
    gpu_memory_limit_str = os.getenv("GPU_MEMORY_LIMIT", "4096")
    # Remove any comments or whitespace
    gpu_memory_limit_str = gpu_memory_limit_str.split('#')[0].strip()
    GPU_MEMORY_LIMIT = int(gpu_memory_limit_str)
except ValueError:
    # If parsing fails, use the default value
    print(f"Warning: Invalid GPU_MEMORY_LIMIT value '{os.getenv('GPU_MEMORY_LIMIT')}'. Using default of 4096.")
    GPU_MEMORY_LIMIT = 4096
CUDA_VISIBLE_DEVICES = os.getenv("CUDA_VISIBLE_DEVICES", "0")

# Batch Processing Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "2"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "analytics_processing.log")

# Set CUDA_VISIBLE_DEVICES environment variable if GPU is enabled
if ENABLE_GPU:
    os.environ["CUDA_VISIBLE_DEVICES"] = CUDA_VISIBLE_DEVICES


def validate_config():
    """Validate that all required configuration variables are set."""
    required_vars = [
        "NOCODB_BASE_URL",
        "NOCODB_API_TOKEN",
        "NOCODB_PROJECT_ID"
    ]
    
    missing_vars = [var for var in required_vars if not globals().get(var)]
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logging.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate S3 configuration if enabled
    if S3_ENABLED:
        s3_required_vars = [
            "S3_BUCKET",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY"
        ]
        
        missing_s3_vars = [var for var in s3_required_vars if not globals().get(var)]
        
        if missing_s3_vars:
            error_msg = f"S3 storage enabled but missing required environment variables: {', '.join(missing_s3_vars)}"
            logging.error(error_msg)
            raise ValueError(error_msg)
    
    # Validate Dify configuration if enabled
    if DIFY_WORKFLOW_ENABLED:
        dify_required_vars = [
            "DIFY_API_KEY",
            "DIFY_BASE_URL"
        ]
        
        missing_dify_vars = [var for var in dify_required_vars if not globals().get(var)]
        
        if missing_dify_vars:
            error_msg = f"Dify workflow enabled but missing required environment variables: {', '.join(missing_dify_vars)}"
            logging.error(error_msg)
            raise ValueError(error_msg)


def setup_logging():
    """Set up logging configuration."""
    numeric_level = getattr(logging, LOG_LEVEL.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_FILE)
        ]
    )
    
    # Reduce verbosity of some loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('s3transfer').setLevel(logging.WARNING)
    logging.getLogger('parquet').setLevel(logging.WARNING)
    
    logging.info(f"Logging initialized at level {LOG_LEVEL}")


# Initialize logging when the module is imported
setup_logging()

# Validate configuration
try:
    validate_config()
    logging.info("Configuration validated successfully")
except ValueError as e:
    logging.warning(f"Configuration validation failed: {str(e)}")
    logging.warning("Continuing with partial configuration")

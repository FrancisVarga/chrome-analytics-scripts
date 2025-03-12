#!/usr/bin/env python
"""
Store Sample Data Script

This script reads all CSV files from the sample_data directory and stores them
in MongoDB and Parquet format using the existing storage modules.

Usage:
    python scripts/store_sample_data.py [--mongodb] [--parquet] [--limit N]
                                       [--parallel] [--workers N] [--use-gpu]

Options:
    --mongodb       Store data in MongoDB (default: True)
    --parquet       Store data in Parquet format (default: True)
    --limit N       Limit the number of records to process (default: no limit)
    --parallel      Use parallel processing (default: False)
    --workers N     Number of worker processes for parallel processing (default: CPU count)
    --use-gpu       Use GPU acceleration when possible (default: False)
"""

import os
import sys
import csv
import json
import argparse
import logging
import uuid
import multiprocessing
from functools import partial
from datetime import datetime
from typing import Dict, List, Any, Optional
from dateutil import parser as date_parser

from analytics_framework.storage.mongodb.client import MongoDBClient
from analytics_framework.storage.parquet_storage import ParquetStorage
from analytics_framework.config import (
    MONGODB_URI,
    MONGODB_DATABASE,
    PARQUET_BASE_DIR,
    PARQUET_PARTITION_BY,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_PAGE_SIZE,
    PARQUET_TARGET_FILE_SIZE_MB,
    PARQUET_MAX_RECORDS_PER_FILE,
    BATCH_SIZE
)

# Create logs directory if it doesn't exist
logs_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs'
)
os.makedirs(logs_dir, exist_ok=True)

# Add parent directory to Python path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler(os.path.join(logs_dir, 'store_sample_data.log'))  # Log to file
    ]
)
logger = logging.getLogger(__name__)

# Constants
SAMPLE_DATA_DIR = 'sample_data'
CONVERSATION_PREFIX = 'conversations_'
MESSAGE_PREFIX = 'messages_'
CHATBOT_PREFIX = 'chatbot_'


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Store sample data in MongoDB and Parquet format'
    )
    parser.add_argument(
        '--mongodb', action='store_true', help='Store data in MongoDB'
    )
    parser.add_argument(
        '--parquet', action='store_true', help='Store data in Parquet format'
    )
    parser.add_argument(
        '--chatbot', action='store_true', help='Process chatbot data'
    )
    parser.add_argument(
        '--limit', type=int, help='Limit the number of records to process'
    )
    parser.add_argument(
        '--parallel', action='store_true', help='Use parallel processing'
    )
    parser.add_argument(
        '--workers', type=int, default=multiprocessing.cpu_count(),
        help='Number of worker processes for parallel processing'
    )
    parser.add_argument(
        '--use-gpu', action='store_true',
        help='Use GPU acceleration when available'
    )
    args = parser.parse_args()

    # Default to both if neither is specified
    if not args.mongodb and not args.parquet:
        args.mongodb = True
        args.parquet = True

    return args


def get_csv_files(directory: str, prefix: str) -> List[str]:
    """
    Get all CSV files with a specific prefix from a directory.
    
    Args:
        directory: Directory to search
        prefix: Prefix to match
        
    Returns:
        List of file paths
    """
    files = []
    for filename in os.listdir(directory):
        if filename.startswith(prefix) and filename.endswith('.csv'):
            files.append(os.path.join(directory, filename))
    return files


def read_csv_file(file_path: str, use_gpu: bool = False) -> List[Dict[str, Any]]:
    """
    Read a CSV file and return a list of dictionaries.
    
    Args:
        file_path: Path to the CSV file
        use_gpu: Whether to use GPU acceleration for reading
        
    Returns:
        List of dictionaries, one for each row
    """
    records = []
    try:
        if use_gpu:
            # Try to use GPU-accelerated reading with PyTorch if available
            try:
                import torch
                import pandas as pd
                logger.info(f"Reading {file_path} with GPU acceleration")
                df = pd.read_csv(file_path)
                # Convert to list of dictionaries
                records = df.to_dict('records')
                # Process all records to convert strings to JSON objects
                records = [parse_json_recursive(record, f"record_{i}") for i, record in enumerate(records)]
                return records
            except (ImportError, ModuleNotFoundError):
                logger.warning("GPU acceleration requested but PyTorch not available. Falling back to CPU.")
            except Exception as e:
                logger.warning(f"Error using GPU acceleration: {str(e)}. Falling back to CPU.")
        
        # Standard CSV reading
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            records = []
            for i, row in enumerate(reader):
                # Process each row to convert strings to JSON objects
                processed_row = parse_json_recursive(row, f"row_{i}")
                records.append(processed_row)
    except Exception as e:
        logger.error(f"Error reading {file_path}: {str(e)}")
    
    return records


def parse_json_recursive(obj: Any, field_name: str = "unknown") -> Any:
    """
    Recursively parse JSON strings within any object.
    
    Args:
        obj: Object to parse (can be dict, list, str, or any other type)
        field_name: Name of the field for logging purposes
        
    Returns:
        Parsed object with all JSON strings converted to Python objects
    """
    if isinstance(obj, dict):
        return {k: parse_json_recursive(v, f"{field_name}.{k}") for k, v in obj.items()}
    elif isinstance(obj, list):
        return [parse_json_recursive(item, f"{field_name}[{i}]") for i, item in enumerate(obj)]
    elif isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            # Recursively parse the result in case it contains more JSON strings
            return parse_json_recursive(parsed, field_name)
        except json.JSONDecodeError:
            return obj
    else:
        return obj

def parse_json_field(json_str: str, field_name: str = "unknown") -> Any:
    """
    Parse a JSON string to a Python object, including nested JSON strings.
    
    Args:
        json_str: JSON string to parse
        field_name: Name of the field for logging purposes
    
    Returns:
        Parsed JSON object or original string if parsing fails
    """
    if not json_str or json_str == '{}' or json_str == '[]':
        return {} if json_str == '{}' else [] if json_str == '[]' else json_str
    
    try:
        # First parse the string as JSON
        parsed = json.loads(str(json_str))
        # Then recursively parse any nested JSON strings
        return parse_json_recursive(parsed, field_name)
    except json.JSONDecodeError:
        logger.warning(f"Could not parse JSON in field '{field_name}': {str(json_str)[:100]}...")
        return json_str


def format_date(date_str: Optional[str]) -> str:
    """
    Parse and format a date string to ISO 8601 format.
    Keeps the original date but ensures it's in a consistent, readable format.
    
    Args:
        date_str: Date string to format
        
    Returns:
        Formatted date string in ISO 8601 format
    """
    if not date_str:
        return datetime.now().isoformat()
    
    try:
        # Try to parse the date string
        parsed_date = date_parser.parse(date_str)
        # Return the date in ISO 8601 format
        return parsed_date.isoformat()
    except (ValueError, TypeError):
        # If parsing fails, return the current date
        logger.warning(f"Could not parse date: {date_str}, using current date instead")
        return datetime.now().isoformat()


def process_conversation_file(file_path: str, limit: Optional[int] = None, use_gpu: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Process a single conversation file.
    
    Args:
        file_path: Path to the conversation file
        limit: Maximum number of conversations to process
        use_gpu: Whether to use GPU acceleration
        
    Returns:
        Dictionary mapping conversation IDs to conversation data
    """
    logger.info(f"Processing conversation file: {file_path}")
    
    records = read_csv_file(file_path, use_gpu=use_gpu)
    logger.info(f"Read {len(records)} records from {file_path}")
    
    # Log a sample record to understand the structure
    if records and len(records) > 0:
        logger.debug(f"Sample conversation record keys: {list(records[0].keys())}")
    
    conversations = {}
    for i, record in enumerate(records):
        # Skip if we've reached the limit
        if limit is not None and i >= limit:
            break
        
        # Get conversation ID - store both 'id' and original ID for matching
        original_id = record.get('id')
        if not original_id:
            # Use app_id as fallback
            original_id = record.get('app_id')
            if not original_id:
                # Generate a new ID if none exists
                original_id = str(uuid.uuid4())
        
        # Use the original ID as the key for the conversations dictionary
        conversation_id = original_id
        
        # Format dates
        created_at = format_date(record.get('created_at'))
        updated_at = format_date(record.get('updated_at')) if record.get('updated_at') else created_at
        
        # Convert to MongoDB format
        conversation = {
            '_id': conversation_id,
            'app_id': record.get('app_id', ''),
            'app_model_config_id': record.get('app_model_config_id', ''),
            'model_provider': record.get('model_provider', ''),
            'model_id': record.get('model_id', ''),
            'mode': record.get('mode', ''),
            'name': record.get('name', ''),
            'summary': record.get('summary', ''),
            'inputs': record.get('inputs', {}),
            'introduction': record.get('introduction', ''),
            'system_instruction': record.get('system_instruction', ''),
            'status': record.get('status', ''),
            'from_source': record.get('from_source', ''),
            'from_end_user_id': record.get('from_end_user_id', ''),
            'from_account_id': record.get('from_account_id', ''),
            'is_deleted': record.get('is_deleted', 'false') == 'true',
            'invoke_from': record.get('invoke_from', ''),
            'dialogue_count': int(record.get('dialogue_count', '0')),
            'messages': [],
            'categories': [],
            'created_at': created_at,
            'updated_at': updated_at
        }
        
        conversations[conversation_id] = conversation
        
    return conversations


def process_conversations(
    conversation_files: List[str],
    limit: Optional[int] = None,
    parallel: bool = False,
    workers: int = None,
    use_gpu: bool = False
) -> Dict[str, Dict[str, Any]]:
    """
    Process conversation files and return a dictionary of conversations.
    
    Args:
        conversation_files: List of conversation file paths
        limit: Maximum number of conversations to process
        parallel: Whether to process files in parallel
        workers: Number of worker processes to use
        use_gpu: Whether to use GPU acceleration
        
    Returns:
        Dictionary mapping conversation IDs to conversation data
    """
    all_conversations = {}
    
    if parallel and len(conversation_files) > 1:
        logger.info(f"Processing {len(conversation_files)} conversation files in parallel with {workers} workers")
        
        # Set per-file limit if global limit is set
        per_file_limit = None
        if limit is not None:
            per_file_limit = max(1, limit // len(conversation_files))
            logger.info(f"Setting per-file limit to {per_file_limit} records")
        
        # Create a partial function with the limit
        process_func = partial(process_conversation_file, limit=per_file_limit, use_gpu=use_gpu)
        
        # Process files in parallel and merge results
        with multiprocessing.Pool(workers) as pool:
            results = pool.map(process_func, conversation_files)
        
        # Combine results
        for file_conversations in results:
            all_conversations.update(file_conversations)
            
        # Apply global limit if needed
        if limit is not None and len(all_conversations) > limit:
            logger.info(f"Limiting total conversations to {limit}")
            all_conversations = dict(list(all_conversations.items())[:limit])
    else:
        # Process sequentially
        processed_count = 0
        
        for file_path in conversation_files:
            file_conversations = process_conversation_file(file_path, limit, use_gpu)
            all_conversations.update(file_conversations)
            
            processed_count += len(file_conversations)
            if limit is not None and processed_count >= limit:
                break
    
    logger.info(f"Processed {len(all_conversations)} unique conversations")
    return all_conversations


def process_message_file(
    file_path: str, 
    conversations: Dict[str, Dict[str, Any]],
    limit: Optional[int] = None,
    use_gpu: bool = False
) -> int:
    """
    Process a single message file and add messages to conversations.
    
    Args:
        file_path: Path to the message file
        conversations: Dictionary of conversations to update
        limit: Maximum number of messages to process
        use_gpu: Whether to use GPU acceleration
        
    Returns:
        Number of messages processed
    """
    logger.info(f"Processing message file: {file_path}")
    
    records = read_csv_file(file_path, use_gpu=use_gpu)
    logger.info(f"Read {len(records)} records from {file_path}")
    
    # Log a sample record to understand the structure
    if records and len(records) > 0:
        logger.debug(f"Sample message record keys: {list(records[0].keys())}")
    
    conversation_ids = set(conversations.keys())
    processed_count = 0
    skipped_count = 0
    matched_count = 0
    
    for record in records:
        # Skip if we've reached the limit
        if limit is not None and processed_count >= limit:
            break
        
        # Get conversation ID - try different possible field names
        conversation_id = None
        
        # First try the standard field name
        if record.get('conversation_id'):
            conversation_id = record.get('conversation_id')
        
        # If not found, check if there's an ID field that might be the conversation ID
        if not conversation_id and record.get('id'):
            # This might be the conversation ID in some formats
            potential_id = record.get('id')
            if potential_id in conversation_ids:
                conversation_id = potential_id
        
        if not conversation_id or conversation_id not in conversation_ids:
            skipped_count += 1
            continue
        
        matched_count += 1
        
        # Format date
        created_at = format_date(record.get('created_at'))
        
        # Convert to MongoDB format
        message = {
            'message_id': str(uuid.uuid4()),
            'app_id': record.get('app_id', ''),
            'model_provider': record.get('model_provider', ''),
            'model_id': record.get('model_id', ''),
            'query': record.get('query', ''),
            'message': record.get('message', {}),
            'message_tokens': int(record.get('message_tokens', '0')),
            'answer': record.get('answer', ''),
            'answer_tokens': int(record.get('answer_tokens', '0')),
            'total_price': float(record.get('total_price', '0')),
            'currency': record.get('currency', 'USD'),
            'from_source': record.get('from_source', ''),
            'from_end_user_id': record.get('from_end_user_id', ''),
            'from_account_id': record.get('from_account_id', ''),
            'status': record.get('status', ''),
            'error': record.get('error', ''),
            'created_at': created_at
        }
        
        # Add message to conversation
        conversations[conversation_id]['messages'].append(message)
        processed_count += 1
    
    # Log statistics about message processing
    logger.info(f"Messages processed: {processed_count}, matched: {matched_count}, skipped: {skipped_count}")
    
    return processed_count


def process_messages(
    message_files: List[str],
    conversations: Dict[str, Dict[str, Any]],
    limit: Optional[int] = None,
    parallel: bool = False,
    workers: int = None,
    use_gpu: bool = False
) -> None:
    """
    Process message files and add messages to conversations.
    
    Args:
        message_files: List of message file paths
        conversations: Dictionary of conversations to update
        limit: Maximum number of messages to process
        parallel: Whether to process files in parallel
        workers: Number of worker processes to use
        use_gpu: Whether to use GPU acceleration
    """
    if parallel and len(message_files) > 1:
        logger.info(f"Processing {len(message_files)} message files in parallel with {workers} workers")
        
        # Set per-file limit if global limit is set
        per_file_limit = None
        if limit is not None:
            per_file_limit = max(1, limit // len(message_files))
            logger.info(f"Setting per-file limit to {per_file_limit} messages")
        
        # Use a manager to share the conversations dictionary between processes
        with multiprocessing.Manager() as manager:
            # Create a shared conversations dictionary
            shared_conversations = manager.dict()
            # Copy the data to the shared dictionary
            for k, v in conversations.items():
                shared_conversations[k] = v
            
            # Create a partial function with fixed arguments
            process_func = partial(process_message_file, 
                                   conversations=shared_conversations,
                                   limit=per_file_limit,
                                   use_gpu=use_gpu)
            
            # Process files in parallel
            with multiprocessing.Pool(workers) as pool:
                results = pool.map(process_func, message_files)
            
            # Get the updated conversations back
            total_processed = sum(results)
            
            # Update the original conversations dict
            for k, v in shared_conversations.items():
                conversations[k] = v
                
            logger.info(f"Processed {total_processed} messages in parallel")
    else:
        # Process sequentially
        processed_count = 0
        
        for file_path in message_files:
            processed = process_message_file(file_path, conversations, limit, use_gpu)
            processed_count += processed
            
            if limit is not None and processed_count >= limit:
                logger.info(f"Reached limit of {limit} messages")
                break
        
        logger.info(f"Processed {processed_count} messages sequentially")


def process_chatbot_file(
    file_path: str, 
    limit: Optional[int] = None,
    use_gpu: bool = False
) -> List[Dict[str, Any]]:
    """
    Process a single chatbot data file.
    
    Args:
        file_path: Path to the chatbot data file
        limit: Maximum number of records to process
        use_gpu: Whether to use GPU acceleration
        
    Returns:
        List of processed chatbot data records
    """
    logger.info(f"Processing chatbot data file: {file_path}")
    
    records = read_csv_file(file_path, use_gpu=use_gpu)
    logger.info(f"Read {len(records)} records from {file_path}")
    
    processed_records = []
    for i, record in enumerate(records):
        # Skip if we've reached the limit
        if limit is not None and i >= limit:
            break
        
        # Format dates
        created_at = format_date(record.get('CreatedAt'))
        updated_at = format_date(record.get('UpdatedAt')) if record.get('UpdatedAt') else created_at
        created_at_dify_date = format_date(record.get('created_at_dify_date'))
        
        # Convert to MongoDB format
        processed_record = {
            "_id": record.get('chatbot_data_id') or str(uuid.uuid4()),
            "original_id": record.get('Id', ''),
            "created_at": created_at,
            "updated_at": updated_at,
            "conversation_id": record.get('conversation_id', ''),
            "translation": record.get('translation', {}),
            "analysis": record.get('analysis', {}),
            "risk_analysis": record.get('risk_analysis', {}),
            "conversational_analysis": record.get('conversational_analysis', {}),
            "recommendations": record.get('recommendations', {}),
            "categorization": record.get('categorization', {}),
            "task_id": record.get('task_id', ''),
            "n8n_data": record.get('n8n_data', {}),
            "success_analysis": record.get('success_analysis', {}),
            "success": record.get('success', ''),
            "success_rating": record.get('success_rating', ''),
            "dify_workflow_id": record.get('dify_workflow_id', ''),
            "click_agent": record.get('click_agent', ''),
            "created_at_dify_date": created_at_dify_date,
            "membercode": record.get('membercode', ''),
            "empty_conversation_id": record.get('empty_conversation_id', '')
        }
        
        processed_records.append(processed_record)
    
    return processed_records


def process_chatbot_data(
    chatbot_files: List[str],
    limit: Optional[int] = None,
    parallel: bool = False,
    workers: int = None,
    use_gpu: bool = False
) -> List[Dict[str, Any]]:
    """
    Process chatbot data files and return a list of chatbot data records.
    
    Args:
        chatbot_files: List of chatbot data file paths
        limit: Maximum number of records to process
        parallel: Whether to process files in parallel
        workers: Number of worker processes to use
        use_gpu: Whether to use GPU acceleration
        
    Returns:
        List of processed chatbot data records
    """
    all_records = []
    
    if parallel and len(chatbot_files) > 1:
        logger.info(f"Processing {len(chatbot_files)} chatbot files in parallel with {workers} workers")
        
        # Set per-file limit if global limit is set
        per_file_limit = None
        if limit is not None:
            per_file_limit = max(1, limit // len(chatbot_files))
            logger.info(f"Setting per-file limit to {per_file_limit} records")
        
        # Create a partial function with the limit
        process_func = partial(process_chatbot_file, limit=per_file_limit, use_gpu=use_gpu)
        
        # Process files in parallel and merge results
        with multiprocessing.Pool(workers) as pool:
            results = pool.map(process_func, chatbot_files)
        
        # Combine results
        for file_records in results:
            all_records.extend(file_records)
            
        # Apply global limit if needed
        if limit is not None and len(all_records) > limit:
            logger.info(f"Limiting total chatbot records to {limit}")
            all_records = all_records[:limit]
    else:
        # Process sequentially
        processed_count = 0
        
        for file_path in chatbot_files:
            file_records = process_chatbot_file(file_path, limit, use_gpu)
            all_records.extend(file_records)
            
            processed_count += len(file_records)
            if limit is not None and processed_count >= limit:
                all_records = all_records[:limit]
                break
    
    logger.info(f"Processed {len(all_records)} chatbot records total")
    return all_records


def store_conversation_batch(batch: List[Dict[str, Any]]) -> None:
    """
    Store a batch of conversations in MongoDB, creating a new client instance per worker.
    
    Args:
        batch: List of conversations to store
    """
    try:
        # Create a new MongoDB client instance for this worker
        mongodb_client = MongoDBClient(
            uri=MONGODB_URI,
            database=MONGODB_DATABASE
        )
        
        for conversation in batch:
            mongodb_client.conversation.save_conversation(conversation)
    except Exception as e:
        logger.error(f"Error storing batch in MongoDB: {str(e)}")

def store_in_mongodb(conversations: Dict[str, Dict[str, Any]], batch_size: int = BATCH_SIZE, parallel: bool = False, workers: int = None) -> None:
    logger.info(f"Storing {len(conversations)} conversations in MongoDB")
    
    # Store conversations in batches
    conversation_list = list(conversations.values())
    if parallel and len(conversation_list) > 1:
        logger.info(f"Storing conversations in parallel with {workers} workers")
        
        # Process batches in parallel
        with multiprocessing.Pool(workers) as pool:
            pool.map(store_conversation_batch, [conversation_list[i:i+batch_size] for i in range(0, len(conversation_list), batch_size)])
    else:
        # Create MongoDB client for sequential processing
        mongodb_client = MongoDBClient(
            uri=MONGODB_URI,
            database=MONGODB_DATABASE
        )
        
        for i in range(0, len(conversation_list), batch_size):
            batch = conversation_list[i:i+batch_size]
            logger.info(f"Storing batch of {len(batch)} conversations in MongoDB")
            
            try:
                # Save each conversation
                for conversation in batch:
                    mongodb_client.conversation.save_conversation(conversation)
                
                logger.info(f"Stored batch of {len(batch)} conversations in MongoDB")
            except Exception as e:
                logger.error(f"Error storing batch in MongoDB: {str(e)}")
    
    logger.info(f"Stored {len(conversations)} conversations in MongoDB")


def store_in_parquet(conversations: Dict[str, Dict[str, Any]], batch_size: int = BATCH_SIZE) -> None:
    """
    Store conversations in Parquet format.
    
    Args:
        conversations: Dictionary of conversations to store
        batch_size: Number of conversations to store in a batch
    """
    logger.info(f"Storing {len(conversations)} conversations in Parquet format")
    
    # Initialize Parquet storage
    parquet_storage = ParquetStorage(
        base_dir=PARQUET_BASE_DIR,
        partition_by=PARQUET_PARTITION_BY,
        compression=PARQUET_COMPRESSION,
        row_group_size=PARQUET_ROW_GROUP_SIZE,
        page_size=PARQUET_PAGE_SIZE,
        target_file_size_mb=PARQUET_TARGET_FILE_SIZE_MB,
        max_records_per_file=PARQUET_MAX_RECORDS_PER_FILE
    )
    
    # Store conversations in batches
    conversation_list = list(conversations.values())
    for i in range(0, len(conversation_list), batch_size):
        batch = conversation_list[i:i+batch_size]
        logger.info(f"Storing batch of {len(batch)} conversations in Parquet format")
        
        try:
            # Store batch
            paths = parquet_storage.store_conversations(batch)
            logger.info(f"Stored batch of {len(batch)} conversations in Parquet format at {paths}")
        except Exception as e:
            logger.error(f"Error storing batch in Parquet format: {str(e)}")
    
    logger.info(f"Stored {len(conversations)} conversations in Parquet format")


def store_chatbot_data_in_mongodb(records: List[Dict[str, Any]], batch_size: int = BATCH_SIZE) -> None:
    """
    Store chatbot data records in MongoDB.
    
    Args:
        records: List of chatbot data records to store
        batch_size: Number of records to store in a batch
    """
    logger.info(f"Storing {len(records)} chatbot records in MongoDB")
    
    # Initialize MongoDB client
    mongodb_client = MongoDBClient(
        uri=MONGODB_URI,
        database=MONGODB_DATABASE
    )
    
    # Create a new collection for chatbot data if it doesn't exist
    collection_name = "analytics_reports"
    
    # Store records in batches
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        logger.info(f"Storing batch of {len(batch)} chatbot records in MongoDB")
        
        try:
            # Insert batch
            for record in batch:
                mongodb_client.base_client.replace_one(
                    collection_name,
                    {"_id": record["_id"]},
                    record,
                    upsert=True
                )
            
            logger.info(f"Stored batch of {len(batch)} chatbot records in MongoDB")
        except Exception as e:
            logger.error(f"Error storing chatbot batch in MongoDB: {str(e)}")
    
    logger.info(f"Stored {len(records)} chatbot records in MongoDB")


def check_gpu_availability() -> bool:
    """
    Check if GPU acceleration is available via PyTorch.
    
    Returns:
        Boolean indicating whether GPU acceleration is available
    """
    try:
        import torch
        logger.info("GPU acceleration is available via PyTorch")
        return True
    except (ImportError, ModuleNotFoundError):
        logger.info("GPU acceleration is not available (PyTorch not installed)")
        return False
    except Exception as e:
        logger.warning(f"Error checking GPU availability: {str(e)}")
        return False


def main():
    """Main function to store sample data."""
    args = parse_args()
    
    logger.info("Starting sample data storage process")
    logger.info(f"MongoDB storage: {args.mongodb}")
    logger.info(f"Parquet storage: {args.parquet}")
    logger.info(f"Process chatbot data: {args.chatbot}")
    logger.info(f"Record limit: {args.limit if args.limit else 'No limit'}")
    logger.info(f"Parallel processing: {args.parallel}")
    if args.parallel:
        logger.info(f"Number of workers: {args.workers}")
    logger.info(f"GPU acceleration: {args.use_gpu}")
    
    # Check GPU availability if requested
    if args.use_gpu:
        gpu_available = check_gpu_availability()
        if not gpu_available:
            logger.warning("GPU acceleration was requested but is not available. Continuing with CPU.")
    
    if args.chatbot:
        # Process chatbot data
        chatbot_files = get_csv_files(SAMPLE_DATA_DIR, CHATBOT_PREFIX)
        logger.info(f"Found {len(chatbot_files)} chatbot data files")
        
        if chatbot_files:
            chatbot_records = process_chatbot_data(
                chatbot_files, 
                args.limit,
                parallel=args.parallel,
                workers=args.workers,
                use_gpu=args.use_gpu
            )
            
            if args.mongodb and chatbot_records:
                store_chatbot_data_in_mongodb(chatbot_records, batch_size=1000)
        else:
            logger.warning(f"No chatbot data files found with prefix '{CHATBOT_PREFIX}' in {SAMPLE_DATA_DIR}")
    
    # Process conversation and message data
    conversation_files = get_csv_files(SAMPLE_DATA_DIR, CONVERSATION_PREFIX)
    message_files = get_csv_files(SAMPLE_DATA_DIR, MESSAGE_PREFIX)
    
    logger.info(f"Found {len(conversation_files)} conversation files")
    logger.info(f"Found {len(message_files)} message files")
    
    # Process conversations
    conversations = process_conversations(
        conversation_files, 
        args.limit,
        parallel=args.parallel,
        workers=args.workers,
        use_gpu=args.use_gpu
    )
    
    # Process messages
    process_messages(
        message_files, 
        conversations, 
        args.limit,
        parallel=args.parallel,
        workers=args.workers,
        use_gpu=args.use_gpu
    )
    
    # Store data
    if args.mongodb:
        store_in_mongodb(conversations, batch_size=1000, parallel=args.parallel, workers=args.workers)
    
    if args.parquet:
        store_in_parquet(conversations)

    logger.info("Sample data storage process completed")


if __name__ == '__main__':
    main()

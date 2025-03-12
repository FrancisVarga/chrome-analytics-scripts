#!/usr/bin/env python
"""
Store Sample Data Script

This script reads all CSV files from the sample_data directory and stores them
in MongoDB and Parquet format using the existing storage modules.

Usage:
    python scripts/store_sample_data.py [--mongodb] [--parquet] [--limit N]

Options:
    --mongodb       Store data in MongoDB (default: True)
    --parquet       Store data in Parquet format (default: True)
    --limit N       Limit the number of records to process (default: no limit)
"""

import os
import sys
import csv
import argparse
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
from dateutil import parser as date_parser


# Create logs directory if it doesn't exist
logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Add parent directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now import modules from analytics_framework
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
CONVERSATION_PREFIX = 'conversations_exported_'
MESSAGE_PREFIX = 'messages_exported_'
CHATBOT_PREFIX = 'chatbot_'


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Store sample data in MongoDB and Parquet format')
    parser.add_argument('--mongodb', action='store_true', help='Store data in MongoDB')
    parser.add_argument('--parquet', action='store_true', help='Store data in Parquet format')
    parser.add_argument('--chatbot', action='store_true', help='Process chatbot data')
    parser.add_argument('--limit', type=int, help='Limit the number of records to process')
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


def read_csv_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Read a CSV file and return a list of dictionaries.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of dictionaries, one for each row
    """
    records = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(row)
    except Exception as e:
        logger.error(f"Error reading {file_path}: {str(e)}")
    
    return records


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


def process_conversations(
    conversation_files: List[str],
    limit: Optional[int] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Process conversation files and return a dictionary of conversations.
    
    Args:
        conversation_files: List of conversation file paths
        limit: Maximum number of conversations to process
        
    Returns:
        Dictionary mapping conversation IDs to conversation data
    """
    conversations = {}
    processed_count = 0
    
    for file_path in conversation_files:
        logger.info(f"Processing conversation file: {file_path}")
        
        records = read_csv_file(file_path)
        logger.info(f"Read {len(records)} records from {file_path}")
        
        for record in records:
            # Skip if we've reached the limit
            if limit is not None and processed_count >= limit:
                break
            
            # Get conversation ID
            conversation_id = record.get('id')
            if not conversation_id:
                # Use app_id as fallback
                conversation_id = record.get('app_id')
                if not conversation_id:
                    # Generate a new ID if none exists
                    conversation_id = str(uuid.uuid4())
            
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
                'inputs': record.get('inputs', '{}'),
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
            processed_count += 1
            
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} conversations")
            
            if limit is not None and processed_count >= limit:
                logger.info(f"Reached limit of {limit} conversations")
                break
        
        if limit is not None and processed_count >= limit:
            break
    
    logger.info(f"Processed {len(conversations)} unique conversations")
    return conversations


def process_messages(
    message_files: List[str],
    conversations: Dict[str, Dict[str, Any]],
    limit: Optional[int] = None
) -> None:
    """
    Process message files and add messages to conversations.
    
    Args:
        message_files: List of message file paths
        conversations: Dictionary of conversations to update
        limit: Maximum number of messages to process
    """
    processed_count = 0
    conversation_ids = set(conversations.keys())
    
    for file_path in message_files:
        logger.info(f"Processing message file: {file_path}")
        
        records = read_csv_file(file_path)
        logger.info(f"Read {len(records)} records from {file_path}")
        
        for record in records:
            # Skip if we've reached the limit
            if limit is not None and processed_count >= limit:
                break
            
            # Get conversation ID
            conversation_id = record.get('conversation_id')
            if not conversation_id:
                continue
            
            # Skip if conversation doesn't exist
            if conversation_id not in conversation_ids:
                continue
            
            # Format date
            created_at = format_date(record.get('created_at'))
            
            # Convert to MongoDB format
            message = {
                'message_id': str(uuid.uuid4()),
                'app_id': record.get('app_id', ''),
                'model_provider': record.get('model_provider', ''),
                'model_id': record.get('model_id', ''),
                'query': record.get('query', ''),
                'message': record.get('message', ''),
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
            
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} messages")
            
            if limit is not None and processed_count >= limit:
                logger.info(f"Reached limit of {limit} messages")
                break
        
        if limit is not None and processed_count >= limit:
            break
    
    logger.info(f"Processed {processed_count} messages")


def store_in_mongodb(conversations: Dict[str, Dict[str, Any]], batch_size: int = BATCH_SIZE) -> None:
    """
    Store conversations in MongoDB.
    
    Args:
        conversations: Dictionary of conversations to store
        batch_size: Number of conversations to store in a batch
    """
    logger.info(f"Storing {len(conversations)} conversations in MongoDB")
    
    # Initialize MongoDB client
    mongodb_client = MongoDBClient(
        uri=MONGODB_URI,
        database=MONGODB_DATABASE
    )
    
    # Store conversations in batches
    conversation_list = list(conversations.values())
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


def process_chatbot_data(
    chatbot_files: List[str],
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Process chatbot data files and return a list of chatbot data records.
    
    Args:
        chatbot_files: List of chatbot data file paths
        limit: Maximum number of records to process
        
    Returns:
        List of processed chatbot data records
    """
    processed_records = []
    processed_count = 0
    
    for file_path in chatbot_files:
        logger.info(f"Processing chatbot data file: {file_path}")
        
        records = read_csv_file(file_path)
        logger.info(f"Read {len(records)} records from {file_path}")
        
        for record in records:
            # Skip if we've reached the limit
            if limit is not None and processed_count >= limit:
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
                "translation": record.get('translation', ''),
                "analysis": record.get('analysis', ''),
                "risk_analysis": record.get('risk_analysis', ''),
                "conversational_analysis": record.get('conversational_analysis', ''),
                "recommendations": record.get('recommendations', ''),
                "categorization": record.get('categorization', ''),
                "task_id": record.get('task_id', ''),
                "n8n_data": record.get('n8n_data', ''),
                "success_analysis": record.get('success_analysis', ''),
                "success": record.get('success', ''),
                "success_rating": record.get('success_rating', ''),
                "dify_workflow_id": record.get('dify_workflow_id', ''),
                "click_agent": record.get('click_agent', ''),
                "created_at_dify_date": created_at_dify_date,
                "membercode": record.get('membercode', ''),
                "empty_conversation_id": record.get('empty_conversation_id', '')
            }
            
            processed_records.append(processed_record)
            processed_count += 1
            
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} chatbot records")
            
            if limit is not None and processed_count >= limit:
                logger.info(f"Reached limit of {limit} chatbot records")
                break
        
        if limit is not None and processed_count >= limit:
            break
    
    logger.info(f"Processed {len(processed_records)} chatbot records total")
    return processed_records


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


def main():
    """Main function to store sample data."""
    args = parse_args()
    
    logger.info("Starting sample data storage process")
    logger.info(f"MongoDB storage: {args.mongodb}")
    logger.info(f"Parquet storage: {args.parquet}")
    logger.info(f"Process chatbot data: {args.chatbot}")
    logger.info(f"Record limit: {args.limit if args.limit else 'No limit'}")
    args.chatbot = True
    
    if args.chatbot:
        # Process chatbot data
        chatbot_files = get_csv_files(SAMPLE_DATA_DIR, CHATBOT_PREFIX)
        logger.info(f"Found {len(chatbot_files)} chatbot data files")
        
        if chatbot_files:
            chatbot_records = process_chatbot_data(chatbot_files, args.limit)
            
            if args.mongodb and chatbot_records:
                store_chatbot_data_in_mongodb(chatbot_records)
        else:
            logger.warning(f"No chatbot data files found with prefix '{CHATBOT_PREFIX}' in {SAMPLE_DATA_DIR}")
    else:
        # Process conversation and message data
        conversation_files = get_csv_files(SAMPLE_DATA_DIR, CONVERSATION_PREFIX)
        message_files = get_csv_files(SAMPLE_DATA_DIR, MESSAGE_PREFIX)
        
        logger.info(f"Found {len(conversation_files)} conversation files")
        logger.info(f"Found {len(message_files)} message files")
        
        # Process conversations
        conversations = process_conversations(conversation_files, args.limit)
        
        # Process messages
        process_messages(message_files, conversations, args.limit)
        
        # Store data
        if args.mongodb:
            store_in_mongodb(conversations)
        
        if args.parquet:
            store_in_parquet(conversations)
    
    logger.info("Sample data storage process completed")


if __name__ == '__main__':
    main()

#!/usr/bin/env python
"""
Store Chatbot Data Script

This script reads chatbot data from a CSV file and stores it in MongoDB.

Usage:
    python scripts/store_chatbot_data.py [--file FILE] [--limit N]

Options:
    --file FILE     Path to the CSV file (default: sample_data/chatbot_data.csv)
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
from analytics_framework.config import (
    MONGODB_URI,
    MONGODB_DATABASE,
    BATCH_SIZE
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler(os.path.join(logs_dir, 'store_chatbot_data.log'))  # Log to file
    ]
)
logger = logging.getLogger(__name__)

# Constants
SAMPLE_DATA_DIR = 'sample_data'
DEFAULT_CSV_FILE = os.path.join(SAMPLE_DATA_DIR, 'chatbot_id_analytics_exported.csv')


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Store chatbot data in MongoDB')
    parser.add_argument('--file', type=str, default=DEFAULT_CSV_FILE, help='Path to the CSV file')
    parser.add_argument('--limit', type=int, help='Limit the number of records to process')
    return parser.parse_args()


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


def process_chatbot_data(
    data: List[Dict[str, Any]],
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Process chatbot data and prepare it for MongoDB storage.
    
    Args:
        data: List of chatbot data records
        limit: Maximum number of records to process
        
    Returns:
        List of processed records ready for MongoDB
    """
    processed_records = []
    processed_count = 0
    
    for record in data:
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
            logger.info(f"Processed {processed_count} records")
    
    logger.info(f"Processed {len(processed_records)} records total")
    return processed_records


def store_in_mongodb(records: List[Dict[str, Any]], batch_size: int = BATCH_SIZE) -> None:
    """
    Store records in MongoDB.
    
    Args:
        records: List of records to store
        batch_size: Number of records to store in a batch
    """
    logger.info(f"Storing {len(records)} records in MongoDB")
    
    # Initialize MongoDB client
    mongodb_client = MongoDBClient(
        uri=MONGODB_URI,
        database=MONGODB_DATABASE
    )
    
    # Create a new collection for chatbot data if it doesn't exist
    collection_name = "chatbot_data"
    
    # Store records in batches
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        logger.info(f"Storing batch of {len(batch)} records in MongoDB")
        
        try:
            # Insert batch
            for record in batch:
                mongodb_client.base_client.replace_one(
                    collection_name,
                    {"_id": record["_id"]},
                    record,
                    upsert=True
                )
            
            logger.info(f"Stored batch of {len(batch)} records in MongoDB")
        except Exception as e:
            logger.error(f"Error storing batch in MongoDB: {str(e)}")
    
    logger.info(f"Stored {len(records)} records in MongoDB")


def main():
    """Main function to store chatbot data."""
    args = parse_args()
    
    logger.info("Starting chatbot data storage process")
    logger.info(f"CSV file: {args.file}")
    logger.info(f"Record limit: {args.limit if args.limit else 'No limit'}")
    
    # Check if file exists
    if not os.path.exists(args.file):
        logger.error(f"File not found: {args.file}")
        return
    
    # Read CSV file
    data = read_csv_file(args.file)
    logger.info(f"Read {len(data)} records from {args.file}")
    
    # Process data
    processed_records = process_chatbot_data(data, args.limit)
    
    # Store data in MongoDB
    store_in_mongodb(processed_records)
    
    logger.info("Chatbot data storage process completed")


if __name__ == '__main__':
    main()

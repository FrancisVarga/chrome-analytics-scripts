#!/usr/bin/env python
"""
Storage module for the store_sample_data module.

This module contains functions for storing data in MongoDB and Parquet format.
"""

import logging
import os
import time
from typing import Dict, List, Any, Optional, Iterable, Generator

from analytics_framework.storage.mongodb.client import MongoDBClient
from analytics_framework.storage.parquet_storage import ParquetStorage
from scripts.store_sample_data.utils import sanitize_mongodb_record, clear_memory, sanitize_error_message
from scripts.store_sample_data.constants import DEFAULT_BATCH_SIZE
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

logger = logging.getLogger(__name__)

def chunk_iterable(iterable: Iterable, size: int) -> Generator[List, None, None]:
    """
    Split an iterable into chunks of specified size.
    
    Args:
        iterable: Iterable to split
        size: Size of each chunk
        
    Yields:
        Chunks of the iterable
    """
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def store_in_mongodb(
    conversations: Dict[str, Dict[str, Any]],
    chatbot_data: List[Dict[str, Any]] = None,
    batch_size: int = BATCH_SIZE
) -> None:
    """
    Store conversations and chatbot data in MongoDB.
    
    Args:
        conversations: Dictionary of conversations to store
        chatbot_data: List of chatbot data records to store
        batch_size: Number of records to store in each batch
    """
    logger.info(f"Storing data in MongoDB at {MONGODB_URI}")
    
    # Initialize MongoDB client
    mongodb_client = MongoDBClient(MONGODB_URI, MONGODB_DATABASE)
    
    # Store conversations in batches
    if conversations:
        logger.info(f"Storing {len(conversations)} conversations in MongoDB")
        conversation_list = list(conversations.values())
        
        # Store in batches
        for i, batch in enumerate(chunk_iterable(conversation_list, batch_size)):
            try:
                # Sanitize all conversations in the batch
                sanitized_batch = [sanitize_mongodb_record(conv) for conv in batch]
                
                # Use bulk operations for better performance
                bulk_operations = []
                for conversation in sanitized_batch:
                    # Create a replace operation for each conversation
                    bulk_operations.append({
                        'replace_one': {
                            'filter': {'_id': conversation['_id']},
                            'replacement': conversation,
                            'upsert': True
                        }
                    })
                
                # Execute bulk operations
                if bulk_operations:
                    # The bulk_write method expects a list of pymongo operations, not dictionaries
                    # Convert the dictionary operations to pymongo operations
                    from pymongo import ReplaceOne
                    pymongo_operations = []
                    for op in bulk_operations:
                        if 'replace_one' in op:
                            replace_params = op['replace_one']
                            pymongo_operations.append(
                                ReplaceOne(
                                    replace_params['filter'],
                                    replace_params['replacement'],
                                    upsert=replace_params.get('upsert', False)
                                )
                            )
                    
                    # Now call bulk_write with the proper operations
                    mongodb_client.base_client.bulk_write(
                        mongodb_client.conversation.collection,
                        pymongo_operations
                    )
                
                logger.info(f"Stored batch {i+1} with {len(batch)} conversations in MongoDB")
                
                # Clear memory after each batch
                clear_memory()
                
            except Exception as e:
                logger.error(f"Error storing conversations batch {i+1} in MongoDB: {sanitize_error_message(str(e))}")
    
    # Store chatbot data in batches
    if chatbot_data:
        logger.info(f"Storing {len(chatbot_data)} chatbot data records in MongoDB")
        
        # Store in batches
        for i, batch in enumerate(chunk_iterable(chatbot_data, batch_size)):
            try:
                # Sanitize all records in the batch
                sanitized_batch = [sanitize_mongodb_record(record) for record in batch]
                
                # Use bulk operations for better performance
                bulk_operations = []
                for record in sanitized_batch:
                    # Create a replace operation for each record
                    bulk_operations.append({
                        'replace_one': {
                            'filter': {'_id': record['_id']},
                            'replacement': record,
                            'upsert': True
                        }
                    })
                
                # Execute bulk operations
                if bulk_operations:
                    # The bulk_write method expects a list of pymongo operations, not dictionaries
                    # Convert the dictionary operations to pymongo operations
                    from pymongo import ReplaceOne
                    pymongo_operations = []
                    for op in bulk_operations:
                        if 'replace_one' in op:
                            replace_params = op['replace_one']
                            pymongo_operations.append(
                                ReplaceOne(
                                    replace_params['filter'],
                                    replace_params['replacement'],
                                    upsert=replace_params.get('upsert', False)
                                )
                            )
                    
                    # Now call bulk_write with the proper operations
                    mongodb_client.base_client.bulk_write(
                        'chatbot_data',
                        pymongo_operations
                    )
                
                logger.info(f"Stored batch {i+1} with {len(batch)} chatbot data records in MongoDB")
                
                # Clear memory after each batch
                clear_memory()
                
            except Exception as e:
                logger.error(f"Error storing chatbot data batch {i+1} in MongoDB: {sanitize_error_message(str(e))}")


def store_in_parquet(
    conversations: Dict[str, Dict[str, Any]],
    chatbot_data: List[Dict[str, Any]] = None,
    batch_size: int = PARQUET_MAX_RECORDS_PER_FILE
) -> None:
    """
    Store conversations and chatbot data in Parquet format.
    
    Args:
        conversations: Dictionary of conversations to store
        chatbot_data: List of chatbot data records to store
        batch_size: Number of records to store in each batch
    """
    logger.info(f"Storing data in Parquet format at {PARQUET_BASE_DIR}")
    
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
    
    # Store conversations
    if conversations:
        logger.info(f"Storing {len(conversations)} conversations in Parquet format")
        conversation_list = list(conversations.values())
        
        # Process in batches to reduce memory usage
        stored_paths = []
        for i, batch in enumerate(chunk_iterable(conversation_list, batch_size)):
            try:
                # Store batch of conversations
                batch_paths = parquet_storage.store_conversations(batch)
                stored_paths.extend(batch_paths)
                
                logger.info(f"Stored batch {i+1} with {len(batch)} conversations in Parquet format")
                
                # Clear memory after each batch
                clear_memory()
                
            except Exception as e:
                logger.error(f"Error storing conversations batch {i+1} in Parquet format: {sanitize_error_message(str(e))}")
        
        if stored_paths:
            logger.info(f"Stored {len(conversation_list)} conversations in Parquet format at {', '.join(stored_paths[:5])}{'...' if len(stored_paths) > 5 else ''}")
    
    # Store chatbot data
    if chatbot_data:
        logger.info(f"Storing {len(chatbot_data)} chatbot data records in Parquet format")
        
        # Process in batches to reduce memory usage
        for i, batch in enumerate(chunk_iterable(chatbot_data, batch_size)):
            try:
                # Use the ParquetStorage class for chatbot data too
                # This is more consistent and leverages the same optimizations
                
                # Get path
                path = os.path.join(PARQUET_BASE_DIR, 'chatbot_data')
                os.makedirs(path, exist_ok=True)
                
                # Generate a unique filename with timestamp
                timestamp = int(time.time())
                filename = f'chatbot_data_batch_{i+1}_{timestamp}.parquet'
                full_path = os.path.join(path, filename)
                
                # Store the batch using ParquetStorage's store_dataframe method
                import pandas as pd
                df = pd.DataFrame(batch)
                parquet_storage.store_dataframe(df, full_path)
                
                logger.info(f"Stored batch {i+1} with {len(batch)} chatbot data records in Parquet format at {full_path}")
                
                # Clear memory after each batch
                clear_memory()
                
            except Exception as e:
                logger.error(f"Error storing chatbot data batch {i+1} in Parquet format: {sanitize_error_message(str(e))}")

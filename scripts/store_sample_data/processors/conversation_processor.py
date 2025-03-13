#!/usr/bin/env python
"""
Conversation processor module.

This module contains functions for processing conversation data.
"""

import logging
import uuid
from typing import Dict, List, Any, Optional, Tuple

from scripts.store_sample_data.file_utils import read_csv_in_chunks
from scripts.store_sample_data.utils import format_date, safe_int_conversion, clear_memory
from scripts.store_sample_data.constants import DEFAULT_BATCH_SIZE

logger = logging.getLogger(__name__)

def process_conversation_record(record: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Process a single conversation record.
    
    Args:
        record: Conversation record to process
        
    Returns:
        Tuple of (conversation_id, processed_conversation)
    """
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
    
    # Safely convert dialogue_count to int
    dialogue_count = safe_int_conversion(record.get('dialogue_count', '0'))
        
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
        'dialogue_count': dialogue_count,
        'messages': [],
        'categories': [],
        'created_at': created_at,
        'updated_at': updated_at
    }
    
    return conversation_id, conversation

def process_conversation_file(
    file_path: str, 
    limit: Optional[int] = None, 
    batch_size: int = DEFAULT_BATCH_SIZE
) -> Dict[str, Dict[str, Any]]:
    """
    Process a single conversation file.
    
    Args:
        file_path: Path to the conversation file
        limit: Maximum number of conversations to process
        batch_size: Size of batches to process at once
        
    Returns:
        Dictionary mapping conversation IDs to conversation data
    """
    logger.info(f"Processing conversation file: {file_path}")
    
    conversations = {}
    processed_count = 0
    
    # Process in batches to reduce memory usage
    for chunk_idx, records in enumerate(read_csv_in_chunks(file_path, batch_size)):
        logger.info(f"Processing chunk {chunk_idx+1} with {len(records)} records from {file_path}")
        
        # Log a sample record to understand the structure (only for the first chunk)
        if chunk_idx == 0 and records and len(records) > 0:
            logger.debug(f"Sample conversation record keys: {list(records[0].keys())}")
        
        for record in records:
            # Skip if we've reached the limit
            if limit is not None and processed_count >= limit:
                break
            
            conversation_id, conversation = process_conversation_record(record)
            conversations[conversation_id] = conversation
            processed_count += 1
        
        # Break if we've reached the limit
        if limit is not None and processed_count >= limit:
            break
    
    logger.info(f"Processed {len(conversations)} conversations from {file_path}")
    return conversations

def process_conversations(
    conversation_files: List[str],
    limit: Optional[int] = None,
    parallel: bool = False,
    workers: int = None,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> Dict[str, Dict[str, Any]]:
    """
    Process conversation files and return a dictionary of conversations.
    
    Args:
        conversation_files: List of conversation file paths
        limit: Maximum number of conversations to process
        parallel: Whether to process files in parallel
        workers: Number of worker processes to use
        batch_size: Size of batches to process at once
        
    Returns:
        Dictionary mapping conversation IDs to conversation data
    """
    if not conversation_files:
        logger.warning("No conversation files to process")
        return {}
    
    all_conversations = {}
    
    # Determine thread count
    thread_count = workers if workers else multiprocessing.cpu_count()
    logger.info(f"Processing {len(conversation_files)} conversation files using {thread_count} threads")
    
    # Process files in parallel if requested
    if parallel and len(conversation_files) > 1:
        import multiprocessing
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            # Submit tasks in batches to avoid memory issues with too many futures
            futures = []
            for file_path in conversation_files:
                future = executor.submit(process_conversation_file, file_path, limit, batch_size)
                futures.append(future)
                
                # If we have enough futures, start processing results
                if len(futures) >= thread_count * 2:
                    for completed in as_completed(futures):
                        file_conversations = completed.result()
                        all_conversations.update(file_conversations)
                        
                        # Check if we've reached the limit
                        if limit is not None and len(all_conversations) >= limit:
                            # Cancel remaining tasks
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            break
                    
                    # Clear processed futures
                    futures = [f for f in futures if not f.done()]
                    
                    # Break if we've reached the limit
                    if limit is not None and len(all_conversations) >= limit:
                        break
            
            # Process any remaining futures
            if futures and not (limit is not None and len(all_conversations) >= limit):
                for completed in as_completed(futures):
                    file_conversations = completed.result()
                    all_conversations.update(file_conversations)
                    
                    # Check if we've reached the limit
                    if limit is not None and len(all_conversations) >= limit:
                        # Cancel remaining tasks
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
    else:
        # Process files sequentially
        for file_path in conversation_files:
            file_conversations = process_conversation_file(file_path, limit, batch_size)
            all_conversations.update(file_conversations)
            
            # Check if we've reached the limit
            if limit is not None and len(all_conversations) >= limit:
                logger.info(f"Reached limit of {limit} conversations")
                break
    
    logger.info(f"Processed {len(all_conversations)} unique conversations")
    return all_conversations

def build_conversation_id_map(conversations: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Build a mapping of conversation IDs to their corresponding conversations.
    
    Args:
        conversations: Dictionary of conversations
        
    Returns:
        Dictionary mapping conversation IDs to conversations
    """
    conversation_id_map = {}
    for conv_id, conv in conversations.items():
        # Add the original ID
        conversation_id_map[conv_id] = conv
        
        # Also add any alternative IDs that might be in the conversation
        if 'app_id' in conv and conv['app_id']:
            conversation_id_map[conv['app_id']] = conv
    
    return conversation_id_map

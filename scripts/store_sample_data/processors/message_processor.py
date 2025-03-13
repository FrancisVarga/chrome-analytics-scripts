#!/usr/bin/env python
"""
Message processor module.

This module contains functions for processing message data.
"""

import logging
import uuid
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Tuple

from scripts.store_sample_data.file_utils import read_csv_in_chunks
from scripts.store_sample_data.utils import format_date, safe_int_conversion, safe_float_conversion, clear_memory
from scripts.store_sample_data.constants import DEFAULT_BATCH_SIZE
from scripts.store_sample_data.processors.conversation_processor import build_conversation_id_map

logger = logging.getLogger(__name__)

def process_message_record(
    record: Dict[str, Any],
    conversation_id_map: Dict[str, Dict[str, Any]]
) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    """
    Process a single message record.
    
    Args:
        record: Message record to process
        conversation_id_map: Mapping of conversation IDs to conversations
        
    Returns:
        Tuple of (success, conversation_id, processed_message)
    """
    # Get conversation ID - try different possible field names
    conversation_id = None
    target_conversation = None
    
    # First try the standard field name
    if record.get('conversation_id'):
        conversation_id = record.get('conversation_id')
        if conversation_id in conversation_id_map:
            target_conversation = conversation_id_map[conversation_id]
    
    # If not found, check if there's an ID field that might be the conversation ID
    if not target_conversation and record.get('id'):
        potential_id = record.get('id')
        if potential_id in conversation_id_map:
            conversation_id = potential_id
            target_conversation = conversation_id_map[potential_id]
    
    # Try app_id as another fallback
    if not target_conversation and record.get('app_id'):
        app_id = record.get('app_id')
        if app_id in conversation_id_map:
            conversation_id = app_id
            target_conversation = conversation_id_map[app_id]
    
    if not target_conversation:
        return False, None, None
    
    # Format date
    created_at = format_date(record.get('created_at'))
    
    # Convert to MongoDB format with safe conversions
    message_tokens = safe_int_conversion(record.get('message_tokens', '0'))
    answer_tokens = safe_int_conversion(record.get('answer_tokens', '0'))
    total_price = safe_float_conversion(record.get('total_price', '0'))
        
    message = {
        'message_id': str(uuid.uuid4()),
        'app_id': record.get('app_id', ''),
        'model_provider': record.get('model_provider', ''),
        'model_id': record.get('model_id', ''),
        'query': record.get('query', ''),
        'message': record.get('message', {}),
        'message_tokens': message_tokens,
        'answer': record.get('answer', ''),
        'answer_tokens': answer_tokens,
        'total_price': total_price,
        'currency': record.get('currency', 'USD'),
        'from_source': record.get('from_source', ''),
        'from_end_user_id': record.get('from_end_user_id', ''),
        'from_account_id': record.get('from_account_id', ''),
        'status': record.get('status', ''),
        'error': record.get('error', ''),
        'created_at': created_at
    }
    
    return True, conversation_id, message


def process_message_file(
    file_path: str, 
    conversations: Dict[str, Dict[str, Any]],
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """
    Process a single message file and add messages to conversations.
    
    Args:
        file_path: Path to the message file
        conversations: Dictionary of conversations to update
        limit: Maximum number of messages to process
        batch_size: Size of batches to process at once
        
    Returns:
        Number of messages processed
    """
    logger.info(f"Processing message file: {file_path}")
    
    # Build conversation ID map once
    conversation_id_map = build_conversation_id_map(conversations)
    
    processed_count = 0
    skipped_count = 0
    matched_count = 0
    
    # Process in batches to reduce memory usage
    for chunk_idx, records in enumerate(read_csv_in_chunks(file_path, batch_size)):
        logger.info(f"Processing chunk {chunk_idx+1} with {len(records)} records from {file_path}")
        
        # Log a sample record to understand the structure (only for the first chunk)
        if chunk_idx == 0 and records and len(records) > 0:
            logger.debug(f"Sample message record keys: {list(records[0].keys())}")
        
        chunk_processed = 0
        chunk_skipped = 0
        chunk_matched = 0
        
        for record in records:
            # Skip if we've reached the limit
            if limit is not None and processed_count >= limit:
                break
            
            # Process the message record
            success, conversation_id, message = process_message_record(record, conversation_id_map)
            
            if success:
                # Add message to conversation
                conversation_id_map[conversation_id]['messages'].append(message)
                processed_count += 1
                chunk_processed += 1
                matched_count += 1
                chunk_matched += 1
            else:
                skipped_count += 1
                chunk_skipped += 1
        
        # Log statistics about chunk processing
        logger.info(f"Chunk {chunk_idx+1} messages processed: {chunk_processed}, matched: {chunk_matched}, skipped: {chunk_skipped}")
        
        # Break if we've reached the limit
        if limit is not None and processed_count >= limit:
            break
    
    # Log statistics about message processing
    logger.info(f"Total messages processed: {processed_count}, matched: {matched_count}, skipped: {skipped_count}")
    
    return processed_count


def process_messages(
    message_files: List[str],
    conversations: Dict[str, Dict[str, Any]],
    limit: Optional[int] = None,
    parallel: bool = False,
    workers: int = None,
    use_gpu: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> None:
    """
    Process message files and add messages to conversations.
    
    Args:
        message_files: List of message file paths
        conversations: Dictionary of conversations to update
        limit: Maximum number of messages to process
        parallel: Whether to process files in parallel
        workers: Number of worker processes to use
        use_gpu: Whether to use GPU acceleration (deprecated, kept for compatibility)
        batch_size: Size of batches to process at once
    """
    if not message_files:
        logger.warning("No message files to process")
        return
    
    if use_gpu:
        try:
            import torch
            if torch.cuda.is_available():
                logger.info("Using GPU acceleration for message processing")
                # Set up GPU context here
                device = torch.device("cuda")
                logger.info(f"Using GPU device: {torch.cuda.get_device_name(0)}")
            else:
                logger.warning("GPU requested but not available. Falling back to CPU.")
                use_gpu = False
        except ImportError:
            logger.warning("PyTorch not installed. GPU acceleration disabled.")
            use_gpu = False
        except Exception as e:
            logger.warning(f"Error setting up GPU: {str(e)}. Falling back to CPU.")
            use_gpu = False
    
    # Determine thread count
    thread_count = workers if workers else multiprocessing.cpu_count()
    logger.info(f"Processing {len(message_files)} message files using {thread_count} threads")
    
    # Process files in parallel if requested
    if parallel and len(message_files) > 1:
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            # Submit tasks in batches to avoid memory issues with too many futures
            futures = []
            for file_path in message_files:
                future = executor.submit(process_message_file, file_path, conversations, limit, batch_size)
                futures.append(future)
                
                # If we have enough futures, start processing results
                if len(futures) >= thread_count * 2:
                    for completed in as_completed(futures):
                        processed = completed.result()
                        
                        # Check if we've reached the limit
                        if limit is not None and processed >= limit:
                            # Cancel remaining tasks
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            break
                    
                    # Clear processed futures
                    futures = [f for f in futures if not f.done()]
                    
                    # Break if we've reached the limit
                    if limit is not None and processed >= limit:
                        break
            
            # Process any remaining futures
            if futures and not (limit is not None and processed >= limit):
                for completed in as_completed(futures):
                    processed = completed.result()
    else:
        # Process files sequentially
        processed_count = 0
        for file_path in message_files:
            processed = process_message_file(file_path, conversations, limit, batch_size)
            processed_count += processed
            
            # Check if we've reached the limit
            if limit is not None and processed_count >= limit:
                logger.info(f"Reached limit of {limit} messages")
                break
    
    # Clear memory after processing all files
    clear_memory()

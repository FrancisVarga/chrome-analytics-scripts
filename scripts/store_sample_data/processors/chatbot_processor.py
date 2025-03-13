#!/usr/bin/env python
"""
Chatbot processor module.

This module contains functions for processing chatbot data.
"""

import logging
import uuid
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional

from scripts.store_sample_data.file_utils import read_csv_in_chunks
from scripts.store_sample_data.utils import format_date, clear_memory
from scripts.store_sample_data.constants import DEFAULT_BATCH_SIZE

logger = logging.getLogger(__name__)

def process_chatbot_file(
    file_path: str, 
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> List[Dict[str, Any]]:
    """
    Process a single chatbot data file.
    
    Args:
        file_path: Path to the chatbot data file
        limit: Maximum number of records to process
        batch_size: Size of batches to process at once
        
    Returns:
        List of processed chatbot data records
    """
    logger.info(f"Processing chatbot data file: {file_path}")
    
    processed_records = []
    processed_count = 0
    
    # Process in batches to reduce memory usage
    for chunk_idx, records in enumerate(read_csv_in_chunks(file_path, batch_size)):
        logger.info(f"Processing chunk {chunk_idx+1} with {len(records)} records from {file_path}")
        
        chunk_records = []
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
            
            chunk_records.append(processed_record)
            processed_count += 1
        
        # Add chunk records to processed records
        processed_records.extend(chunk_records)
        logger.info(f"Processed {len(chunk_records)} records from chunk {chunk_idx+1}")
        
        # Break if we've reached the limit
        if limit is not None and processed_count >= limit:
            break
    
    logger.info(f"Processed {len(processed_records)} records from {file_path}")
    return processed_records


def process_chatbot_data(
    chatbot_files: List[str],
    limit: Optional[int] = None,
    parallel: bool = False,
    workers: int = None,
    use_gpu: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> List[Dict[str, Any]]:
    """
    Process chatbot data files and return a list of chatbot data records.
    
    Args:
        chatbot_files: List of chatbot data file paths
        limit: Maximum number of records to process
        parallel: Whether to process files in parallel
        workers: Number of worker processes to use
        use_gpu: Whether to use GPU acceleration (deprecated, kept for compatibility)
        batch_size: Size of batches to process at once
        
    Returns:
        List of processed chatbot data records
    """
    if not chatbot_files:
        logger.warning("No chatbot files to process")
        return []
    
    if use_gpu:
        try:
            import torch
            if torch.cuda.is_available():
                logger.info("Using GPU acceleration for chatbot data processing")
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
    
    all_records = []
    
    # Determine thread count
    thread_count = workers if workers else multiprocessing.cpu_count()
    logger.info(f"Processing {len(chatbot_files)} chatbot files using {thread_count} threads")
    
    # Process files in parallel if requested
    if parallel and len(chatbot_files) > 1:
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            # Submit tasks in batches to avoid memory issues with too many futures
            futures = []
            for file_path in chatbot_files:
                future = executor.submit(process_chatbot_file, file_path, limit, batch_size)
                futures.append(future)
                
                # If we have enough futures, start processing results
                if len(futures) >= thread_count * 2:
                    for completed in as_completed(futures):
                        file_records = completed.result()
                        all_records.extend(file_records)
                        
                        # Check if we've reached the limit
                        if limit is not None and len(all_records) >= limit:
                            # Cancel remaining tasks
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            break
                    
                    # Clear processed futures
                    futures = [f for f in futures if not f.done()]
                    
                    # Break if we've reached the limit
                    if limit is not None and len(all_records) >= limit:
                        break
            
            # Process any remaining futures
            if futures and not (limit is not None and len(all_records) >= limit):
                for completed in as_completed(futures):
                    file_records = completed.result()
                    all_records.extend(file_records)
                    
                    # Check if we've reached the limit
                    if limit is not None and len(all_records) >= limit:
                        # Cancel remaining tasks
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
    else:
        # Process files sequentially
        for file_path in chatbot_files:
            file_records = process_chatbot_file(file_path, limit, batch_size)
            all_records.extend(file_records)
            
            # Check if we've reached the limit
            if limit is not None and len(all_records) >= limit:
                logger.info(f"Reached limit of {limit} chatbot records")
                break
    
    # Trim to limit if needed
    if limit is not None and len(all_records) > limit:
        all_records = all_records[:limit]
    
    logger.info(f"Processed {len(all_records)} chatbot records total")
    
    # Clear memory after processing all files
    clear_memory()
    
    return all_records

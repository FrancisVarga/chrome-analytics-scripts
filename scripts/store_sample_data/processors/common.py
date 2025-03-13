#!/usr/bin/env python
"""
Common utilities for data processors.

This module contains common functions and utilities used by all data processors.
"""

import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Callable, TypeVar, Generic

from scripts.store_sample_data.utils import clear_memory

logger = logging.getLogger(__name__)

T = TypeVar('T')
R = TypeVar('R')

def process_in_parallel(
    items: List[T],
    process_func: Callable[[T, Optional[int], int], R],
    limit: Optional[int] = None,
    workers: Optional[int] = None,
    batch_size: int = 1000,
    description: str = "items"
) -> List[R]:
    """
    Process items in parallel using a thread pool.
    
    Args:
        items: List of items to process
        process_func: Function to process each item
        limit: Maximum number of items to process
        workers: Number of worker threads
        batch_size: Size of batches for processing
        description: Description of items for logging
        
    Returns:
        List of processed results
    """
    if not items:
        logger.warning(f"No {description} to process")
        return []
    
    results = []
    
    # Determine thread count
    thread_count = workers if workers else multiprocessing.cpu_count()
    logger.info(f"Processing {len(items)} {description} using {thread_count} threads")
    
    # Process in parallel
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        # Submit tasks in batches to avoid memory issues with too many futures
        futures = []
        for item in items:
            future = executor.submit(process_func, item, limit, batch_size)
            futures.append(future)
            
            # If we have enough futures, start processing results
            if len(futures) >= thread_count * 2:
                for completed in as_completed(futures):
                    result = completed.result()
                    results.append(result)
                    
                    # Check if we've reached the limit
                    if limit is not None and len(results) >= limit:
                        # Cancel remaining tasks
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
                
                # Clear processed futures
                futures = [f for f in futures if not f.done()]
                
                # Break if we've reached the limit
                if limit is not None and len(results) >= limit:
                    break
        
        # Process any remaining futures
        if futures and not (limit is not None and len(results) >= limit):
            for completed in as_completed(futures):
                result = completed.result()
                results.append(result)
                
                # Check if we've reached the limit
                if limit is not None and len(results) >= limit:
                    # Cancel remaining tasks
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break
    
    # Clear memory after processing
    clear_memory()
    
    return results

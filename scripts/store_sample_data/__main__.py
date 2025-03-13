#!/usr/bin/env python
"""
Main entry point for the store_sample_data module.

This module handles argument parsing and the main execution flow.
"""

import os
import sys
import argparse
import logging
import time
from typing import Dict, List, Any, Optional

# Add parent directory to Python path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../..')
))

from scripts.store_sample_data.constants import (
    SAMPLE_DATA_DIR,
    CONVERSATION_PREFIX,
    MESSAGE_PREFIX,
    CHATBOT_PREFIX,
    DEFAULT_BATCH_SIZE
)
from scripts.store_sample_data.utils import setup_logging, clear_memory, check_gpu_availability, configure_gpu_settings
from scripts.store_sample_data.file_utils import get_csv_files
from scripts.store_sample_data.processors import (
    process_conversations,
    process_messages,
    process_chatbot_data
)
from scripts.store_sample_data.storage import (
    store_in_mongodb,
    store_in_parquet
)
from analytics_framework.config import BATCH_SIZE


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
        '--workers', type=int, default=os.cpu_count(),
        help='Number of worker processes for parallel processing'
    )
    parser.add_argument(
        '--use-gpu', action='store_true',
        help='Use GPU acceleration when available (deprecated)'
    )
    parser.add_argument(
        '--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
        help=f'Batch size for processing (default: {DEFAULT_BATCH_SIZE})'
    )
    args = parser.parse_args()

    # Default to both if neither is specified
    if not args.mongodb and not args.parquet:
        args.mongodb = True
        args.parquet = True

    # Check if GPU should be used
    if args.use_gpu:
        print("Checking GPU availability...")
    else:
        print("GPU acceleration disabled by user. Using CPU for processing.")

    return args


def main():
    """Main entry point for the script."""
    start_time = time.time()
    
    # Parse command line arguments
    args = parse_args()
    
    # Setup logging
    logger = setup_logging('store_sample_data.log')
    
    # Log the arguments
    logger.info(f"Arguments: {args}")
    
    # Check for GPU availability if requested
    use_gpu = False
    if args.use_gpu:
        use_gpu = check_gpu_availability()
        if use_gpu:
            # Configure GPU settings
            # You could read these from environment variables or config
            memory_limit = os.environ.get('GPU_MEMORY_LIMIT')
            visible_devices = os.environ.get('CUDA_VISIBLE_DEVICES')
            
            if memory_limit:
                # Extract just the numeric part from the memory limit string
                import re
                memory_limit_match = re.match(r'(\d+)', memory_limit)
                if memory_limit_match:
                    memory_limit = int(memory_limit_match.group(1))
                else:
                    logger.warning(f"Invalid GPU memory limit format: {memory_limit}. Using default.")
                    memory_limit = None
            
            configure_gpu_settings(
                memory_limit=memory_limit,
                visible_devices=visible_devices
            )
        else:
            logger.info("GPU acceleration not available. Falling back to CPU processing.")
    
    # Get CSV files
    conversation_files = get_csv_files(SAMPLE_DATA_DIR, CONVERSATION_PREFIX)
    message_files = get_csv_files(SAMPLE_DATA_DIR, MESSAGE_PREFIX)
    chatbot_files = get_csv_files(SAMPLE_DATA_DIR, CHATBOT_PREFIX) if args.chatbot else []
    
    # Log the files found
    logger.info(f"Found {len(conversation_files)} conversation files")
    logger.info(f"Found {len(message_files)} message files")
    if args.chatbot:
        logger.info(f"Found {len(chatbot_files)} chatbot files")
    
    # Process conversations
    logger.info("Processing conversations...")
    conversations = process_conversations(
        conversation_files,
        limit=args.limit,
        parallel=args.parallel,
        workers=args.workers,
        batch_size=args.batch_size
    )
    
    # Process messages
    if conversations:
        logger.info("Processing messages...")
        process_messages(
            message_files,
            conversations,
            limit=args.limit,
            parallel=args.parallel,
            workers=args.workers,
            use_gpu=use_gpu,  # Use GPU if available and requested
            batch_size=args.batch_size
        )
    
    # Process chatbot data
    chatbot_data = None
    if args.chatbot:
        logger.info("Processing chatbot data...")
        chatbot_data = process_chatbot_data(
            chatbot_files,
            limit=args.limit,
            parallel=args.parallel,
            workers=args.workers,
            use_gpu=use_gpu,  # Use GPU if available and requested
            batch_size=args.batch_size
        )
    
    # Free up memory before storage operations
    clear_memory()
    
    # Store data in MongoDB
    if args.mongodb:
        logger.info("Storing data in MongoDB...")
        store_in_mongodb(
            conversations, 
            chatbot_data,
            batch_size=BATCH_SIZE
        )
    
    # Store data in Parquet format
    if args.parquet:
        logger.info("Storing data in Parquet format...")
        store_in_parquet(
            conversations, 
            chatbot_data
        )
    
    # Calculate and log execution time
    execution_time = time.time() - start_time
    logger.info(f"Done! Total execution time: {execution_time:.2f} seconds")


if __name__ == '__main__':
    main()

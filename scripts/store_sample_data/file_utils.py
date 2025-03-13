#!/usr/bin/env python
"""
File utilities for the store_sample_data module.

This module contains functions for reading CSV files and other file operations.
"""

import os
import logging
import csv
from typing import Dict, List, Any, Optional, Generator, Iterable

import pandas as pd

from scripts.store_sample_data.utils import parse_json_recursive
from scripts.store_sample_data.constants import DEFAULT_BATCH_SIZE

logger = logging.getLogger(__name__)

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
    Read a CSV file using pandas and return a list of dictionaries.
    
    Args:
        file_path: Path to the CSV file
        use_gpu: Whether to use GPU acceleration for reading (deprecated, kept for compatibility)
        
    Returns:
        List of dictionaries, one for each row
    """
    if use_gpu:
        logger.warning("GPU acceleration for CSV reading is deprecated and will be ignored")
    
    records = []
    try:
        # Read CSV file with pandas
        df = pd.read_csv(
            file_path,
            encoding='utf-8',
            low_memory=False,
            on_bad_lines='warn',  # Skip lines with errors and warn about them
            dtype_backend='numpy_nullable'  # More memory efficient
        )
        
        # Convert to list of dictionaries using pandas
        # This is more efficient than iterating through rows
        records = df.to_dict('records')
        
        # Process all records to convert strings to JSON objects
        records = [parse_json_recursive(record, f"record_{i}") for i, record in enumerate(records)]
        
        logger.info(f"Successfully read {len(records)} records from {file_path}")
        
    except Exception as e:
        logger.error(f"Error reading {file_path} with pandas: {str(e)}")
        logger.info("Falling back to standard CSV reading")
        
        # Fallback to standard CSV reading
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                records = []
                for i, row in enumerate(reader):
                    # Process each row to convert strings to JSON objects
                    processed_row = parse_json_recursive(row, f"row_{i}")
                    records.append(processed_row)
        except Exception as fallback_error:
            logger.error(f"Error in fallback CSV reading: {str(fallback_error)}")
    
    return records


def read_csv_in_chunks(file_path: str, chunk_size: int = DEFAULT_BATCH_SIZE) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Read a CSV file in chunks to reduce memory usage.
    
    Args:
        file_path: Path to the CSV file
        chunk_size: Number of rows to read at once
        
    Yields:
        Lists of dictionaries, each containing chunk_size rows or fewer
    """
    try:
        # Use pandas to read the file in chunks
        for chunk in pd.read_csv(
            file_path,
            encoding='utf-8',
            low_memory=False,
            on_bad_lines='warn',
            chunksize=chunk_size,
            dtype_backend='numpy_nullable'
        ):
            # Convert chunk to list of dictionaries
            records = chunk.to_dict('records')
            
            # Process records to convert strings to JSON objects
            processed_records = [parse_json_recursive(record, f"chunk_record_{i}") for i, record in enumerate(records)]
            
            yield processed_records
            
    except Exception as e:
        logger.error(f"Error reading {file_path} in chunks with pandas: {str(e)}")
        logger.info("Falling back to standard CSV reading")
        
        # Fallback to standard CSV reading in chunks
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                
                chunk = []
                for i, row in enumerate(reader):
                    # Process the row to convert strings to JSON objects
                    processed_row = parse_json_recursive(row, f"chunk_row_{i}")
                    chunk.append(processed_row)
                    
                    # If we've reached the chunk size, yield the chunk and start a new one
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                
                # Yield any remaining rows
                if chunk:
                    yield chunk
                    
        except Exception as fallback_error:
            logger.error(f"Error in fallback CSV chunk reading: {str(fallback_error)}")
            yield []  # Return empty list to avoid breaking the caller

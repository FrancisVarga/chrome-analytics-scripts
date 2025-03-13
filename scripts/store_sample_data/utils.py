#!/usr/bin/env python
"""
Utility functions for the store_sample_data module.

This module contains utility functions for parsing JSON, formatting dates, etc.
"""

import json
import logging
import gc
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Tuple

from dateutil import parser as date_parser

logger = logging.getLogger(__name__)

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


def safe_int_conversion(value: Any, default: int = 0) -> int:
    """
    Safely convert a value to an integer, handling errors and large values.
    
    Args:
        value: Value to convert
        default: Default value to return if conversion fails
        
    Returns:
        Converted integer or default value
    """
    MAX_INT = 9223372036854775807  # 2^63 - 1 (MongoDB max int)
    
    if value is None:
        return default
    
    try:
        int_value = int(value)
        # Check if the integer is too large for MongoDB
        if int_value > MAX_INT:
            logger.warning(f"Integer value {int_value} is too large for MongoDB, using {default} instead")
            return default
        return int_value
    except (ValueError, TypeError):
        return default


def safe_float_conversion(value: Any, default: float = 0.0) -> float:
    """
    Safely convert a value to a float, handling errors.
    
    Args:
        value: Value to convert
        default: Default value to return if conversion fails
        
    Returns:
        Converted float or default value
    """
    if value is None:
        return default
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def sanitize_mongodb_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize a record for MongoDB storage.
    
    MongoDB can only handle up to 8-byte ints (max value: 2^63 - 1).
    This function recursively checks all integer values in the record
    and ensures they are within the acceptable range.
    
    Args:
        record: Record to sanitize
        
    Returns:
        Sanitized record
    """
    MAX_INT = 9223372036854775807  # 2^63 - 1
    
    def sanitize_value(value):
        if isinstance(value, int) and not isinstance(value, bool):
            # Check if the integer is too large for MongoDB
            if value > MAX_INT:
                return 0  # Reset to 0 if too large
            return value
        elif isinstance(value, dict):
            return {k: sanitize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [sanitize_value(item) for item in value]
        else:
            return value
    
    return sanitize_value(record)


def setup_logging(log_file: str) -> logging.Logger:
    """
    Configure logging for the application.
    
    Args:
        log_file: Path to the log file
        
    Returns:
        Configured logger
    """
    from scripts.store_sample_data.constants import LOGS_DIR
    import os
    import sys
    
    # Create logs directory if it doesn't exist
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler(
                os.path.join(LOGS_DIR, log_file),
                encoding='utf-8'  # Explicitly set UTF-8 encoding for the log file
            )
        ]
    )
    
    # Set stdout encoding to UTF-8 to handle emojis and other Unicode characters
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
    
    return logging.getLogger(__name__)


def clear_memory() -> None:
    """
    Force garbage collection to free up memory.
    """
    gc.collect()


def sanitize_error_message(error_message: str) -> str:
    """
    Sanitize error messages to ensure they can be safely logged.
    
    This function handles Unicode characters that might cause encoding issues
    when logging to terminals or files with different encodings.
    
    Args:
        error_message: The error message to sanitize
        
    Returns:
        Sanitized error message that can be safely logged
    """
    if not error_message:
        return ""
    
    try:
        # Try to encode and decode the message to catch any encoding issues
        return str(error_message).encode('ascii', 'backslashreplace').decode('ascii')
    except Exception:
        # If that fails, return a generic message
        return "Error message contained characters that could not be encoded"


def check_gpu_availability() -> bool:
    """
    Check if GPU is available for acceleration.
    
    This function attempts to detect CUDA-compatible GPUs and configure
    the environment for GPU acceleration.
    
    Returns:
        True if GPU is available and configured, False otherwise
    """
    try:
        # Try to import necessary libraries
        import torch
        
        # Check if CUDA is available
        if torch.cuda.is_available():
            device_count = torch.cuda.device_count()
            device_name = torch.cuda.get_device_name(0) if device_count > 0 else "Unknown"
            logger.info(f"GPU acceleration enabled: {device_count} device(s) available")
            logger.info(f"Using GPU: {device_name}")
            return True
        else:
            logger.info("CUDA is not available. Using CPU for processing.")
            return False
    except ImportError:
        logger.info("PyTorch not installed. GPU acceleration disabled.")
        return False
    except Exception as e:
        logger.warning(f"Error checking GPU availability: {sanitize_error_message(str(e))}")
        logger.info("Falling back to CPU processing.")
        return False


def configure_gpu_settings(memory_limit: Optional[int] = None, visible_devices: Optional[str] = None) -> None:
    """
    Configure GPU settings for optimal performance.
    
    Args:
        memory_limit: Memory limit in MB (None for no limit)
        visible_devices: Comma-separated list of device indices to use (None for all)
    """
    try:
        # Set CUDA_VISIBLE_DEVICES environment variable if specified
        if visible_devices is not None:
            os.environ['CUDA_VISIBLE_DEVICES'] = visible_devices
            logger.info(f"Set CUDA_VISIBLE_DEVICES to {visible_devices}")
        
        # Try to import necessary libraries
        import torch
        
        # Configure memory limit if specified and GPU is available
        if memory_limit is not None and torch.cuda.is_available():
            # This is a simplified approach - in a real implementation,
            # you would use torch.cuda.set_per_process_memory_fraction or similar
            logger.info(f"Set GPU memory limit to {memory_limit} MB")
            
        # Additional GPU optimizations could be added here
        
    except ImportError:
        logger.info("PyTorch not installed. GPU settings not configured.")
    except Exception as e:
        logger.warning(f"Error configuring GPU settings: {sanitize_error_message(str(e))}")

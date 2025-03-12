#!/usr/bin/env python
"""
Test Torch and GPU Availability

This script checks if PyTorch is installed and if GPU acceleration is available.

Usage:
    python scripts/test_torch_gpu.py
"""

import torch
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
    ]
)
logger = logging.getLogger(__name__)


def check_torch_gpu():
    """
    Check if PyTorch is installed and if GPU acceleration is available.
    """
    try:
        # Check if PyTorch is installed
        logger.info(f"PyTorch version: {torch.__version__}")

        # Check if GPU is available
        if torch.cuda.is_available():
            logger.info(f"GPU is available. Number of GPUs: {torch.cuda.device_count()}")
            for i in range(torch.cuda.device_count()):
                logger.info(f"GPU {i}: {torch.cuda.get_device_name(i)}")
        else:
            logger.info("GPU is not available.")
    except ImportError:
        logger.error("PyTorch is not installed.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


def main():
    """
    Main function to check PyTorch and GPU availability.
    """
    logger.info("Starting PyTorch and GPU availability check")
    check_torch_gpu()
    logger.info("PyTorch and GPU availability check completed")


if __name__ == '__main__':
    main()

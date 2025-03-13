#!/usr/bin/env python
"""
Store Sample Data Script

This script reads all CSV files from the sample_data directory and stores them
in MongoDB and Parquet format using the existing storage modules.

Usage:
    python scripts/store_sample_data.py [--mongodb] [--parquet] [--limit N]
                                       [--parallel] [--workers N] [--use-gpu]

Options:
    --mongodb       Store data in MongoDB (default: True)
    --parquet       Store data in Parquet format (default: True)
    --limit N       Limit the number of records to process (default: no limit)
    --parallel      Use parallel processing (default: False)
    --workers N     Number of worker processes for parallel processing (default: CPU count)
    --use-gpu       Use GPU acceleration when possible (default: False)
"""

import os
import sys

# Add parent directory to Python path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))

from scripts.store_sample_data.__main__ import main

if __name__ == '__main__':
    main()

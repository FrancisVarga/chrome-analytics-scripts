# Store Sample Data

This module reads all CSV files from the sample_data directory and stores them in MongoDB and Parquet format using the existing storage modules.

## Usage

```bash
python scripts/store_sample_data.py [--mongodb] [--parquet] [--chatbot] [--limit N] [--parallel] [--workers N] [--use-gpu]
```

### Options

- `--mongodb`: Store data in MongoDB (default: True if neither --mongodb nor --parquet is specified)
- `--parquet`: Store data in Parquet format (default: True if neither --mongodb nor --parquet is specified)
- `--chatbot`: Process chatbot data (default: False)
- `--limit N`: Limit the number of records to process (default: no limit)
- `--parallel`: Use parallel processing (default: False)
- `--workers N`: Number of worker processes for parallel processing (default: CPU count)
- `--use-gpu`: Use GPU acceleration when possible (default: False)

## Module Structure

The script has been modularized into the following components:

- `__main__.py`: Entry point with argument parsing and main execution flow
- `constants.py`: Constants used across modules
- `utils.py`: Utility functions for parsing JSON, formatting dates, etc.
- `file_utils.py`: Functions for reading CSV files
- `data_processors.py`: Functions for processing conversations, messages, and chatbot data
- `storage.py`: Functions for storing data in MongoDB and Parquet format

## Data Flow

1. Parse command line arguments
2. Get CSV files from the sample_data directory
3. Process conversations
4. Process messages and add them to conversations
5. Process chatbot data (if --chatbot is specified)
6. Store data in MongoDB (if --mongodb is specified)
7. Store data in Parquet format (if --parquet is specified)

## Example

```bash
# Store data in both MongoDB and Parquet format
python scripts/store_sample_data.py

# Store data only in MongoDB
python scripts/store_sample_data.py --mongodb

# Store data only in Parquet format
python scripts/store_sample_data.py --parquet

# Process chatbot data
python scripts/store_sample_data.py --chatbot

# Limit the number of records to process
python scripts/store_sample_data.py --limit 100

# Use parallel processing
python scripts/store_sample_data.py --parallel

# Use GPU acceleration
python scripts/store_sample_data.py --use-gpu

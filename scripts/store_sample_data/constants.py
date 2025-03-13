#!/usr/bin/env python
"""
Constants for the store_sample_data module.

This module contains constants used across the store_sample_data modules.
"""

import os

# Directories
SAMPLE_DATA_DIR = 'sample_data'
LOGS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs'
)

# File prefixes
CONVERSATION_PREFIX = 'conversations_'
MESSAGE_PREFIX = 'messages_'
CHATBOT_PREFIX = 'chatbot_'

# Batch processing
DEFAULT_BATCH_SIZE = 5000

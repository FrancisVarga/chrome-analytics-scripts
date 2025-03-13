#!/usr/bin/env python
"""
Data processors for the store_sample_data module.

This module is maintained for backward compatibility.
It imports and re-exports functions from the processors package.
"""

import logging
import warnings

# Import from the new modular structure
from scripts.store_sample_data.processors.conversation_processor import (
    process_conversation_record,
    process_conversation_file,
    process_conversations,
    build_conversation_id_map
)

from scripts.store_sample_data.processors.message_processor import (
    process_message_record,
    process_message_file,
    process_messages
)

from scripts.store_sample_data.processors.chatbot_processor import (
    process_chatbot_file,
    process_chatbot_data
)

# Show a deprecation warning
warnings.warn(
    "The data_processors module is deprecated and will be removed in a future version. "
    "Please use the processors package instead.",
    DeprecationWarning,
    stacklevel=2
)

# Export all functions for backward compatibility
__all__ = [
    'process_conversation_record',
    'process_conversation_file',
    'process_conversations',
    'build_conversation_id_map',
    'process_message_record',
    'process_message_file',
    'process_messages',
    'process_chatbot_file',
    'process_chatbot_data'
]

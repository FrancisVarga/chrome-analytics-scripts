"""
Functions for creating conversation analytics report views.

This module contains functions for creating MongoDB views that attach
analytics reports to conversations, allowing them to be fetched via conversation_id.
"""

import logging
from analytics_framework.config import (
    MONGODB_CONVERSATIONS_COLLECTION,
    MONGODB_ANALYTICS_REPORTS_COLLECTION
)
from scripts.mongodb.utils import create_view

logger = logging.getLogger(__name__)


def create_conversation_with_analytics_report_view(client):
    """
    Create a view that attaches analytics reports to conversations,
    allowing them to be fetched via conversation_id.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "conversation_with_analytics_report"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False
            }
        },
        {
            # Lookup analytics reports for this conversation
            "$lookup": {
                "from": MONGODB_ANALYTICS_REPORTS_COLLECTION,
                "localField": "_id",
                "foreignField": "conversation_id",
                "as": "analytics_report"
            }
        },
        {
            # Unwind the analytics_report array to get the first report
            # If there's no report, it will preserve the document with null
            "$unwind": {
                "path": "$analytics_report", 
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$project": {
                "_id": 1,
                "app_id": 1,
                "model_provider": 1,
                "model_id": 1,
                "mode": 1,
                "name": 1,
                "summary": 1,
                "from_end_user_id": 1,
                "from_account_id": 1,
                "status": 1,
                "created_at": 1,
                "updated_at": 1,
                "is_deleted": 1,
                "message_count": 1,
                "total_tokens": 1,
                "total_price": 1,
                "currency": 1,
                "system_instruction": 1,
                "system_instruction_tokens": 1,
                "analytics_metadata": 1,
                "messages": 1,
                "categories": 1,
                "analytics_report": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)

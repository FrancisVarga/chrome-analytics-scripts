"""
Functions for creating user metrics views.

This module contains functions for creating MongoDB views related to user
activity and metrics.
"""

import logging
from analytics_framework.config import MONGODB_CONVERSATIONS_COLLECTION
from scripts.mongodb.utils import create_view

logger = logging.getLogger(__name__)


def create_user_activity_metrics_view(client):
    """
    Create a view for user activity metrics.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "user_activity_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False,
                "from_end_user_id": {"$ne": None}
            }
        },
        {
            "$project": {
                "date": {
                    "$dateFromString": {
                        "dateString": "$created_at"
                    }
                },
                "from_end_user_id": 1,
                "message_count": 1,
                "total_tokens": 1,
                "total_price": 1
            }
        },
        {
            "$group": {
                "_id": "$from_end_user_id",
                "conversation_count": {"$sum": 1},
                "total_messages": {"$sum": "$message_count"},
                "total_tokens": {"$sum": "$total_tokens"},
                "total_price": {"$sum": "$total_price"},
                "first_conversation_at": {"$min": "$date"},
                "last_conversation_at": {"$max": "$date"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "user_id": "$_id",
                "conversation_count": 1,
                "total_messages": 1,
                "total_tokens": 1,
                "total_price": 1,
                "first_conversation_at": 1,
                "last_conversation_at": 1,
                "average_messages_per_conversation": {
                    "$divide": ["$total_messages", "$conversation_count"]
                },
                "average_tokens_per_conversation": {
                    "$divide": ["$total_tokens", "$conversation_count"]
                },
                "average_price_per_conversation": {
                    "$divide": ["$total_price", "$conversation_count"]
                },
                "days_active": {
                    "$divide": [
                        {
                            "$subtract": [
                                "$last_conversation_at", 
                                "$first_conversation_at"
                            ]
                        },
                        86400000  # milliseconds in a day
                    ]
                }
            }
        },
        {
            "$sort": {
                "conversation_count": -1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)

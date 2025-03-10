"""
Functions for creating model metrics views.

This module contains functions for creating MongoDB views related to model
usage and token metrics.
"""

import logging
from analytics_framework.config import MONGODB_CONVERSATIONS_COLLECTION
from scripts.mongodb.utils import create_view

logger = logging.getLogger(__name__)


def create_model_usage_metrics_view(client):
    """
    Create a view for model usage metrics.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "model_usage_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False
            }
        },
        {
            "$project": {
                "date": {
                    "$dateFromString": {
                        "dateString": "$created_at"
                    }
                },
                "model_provider": 1,
                "model_id": 1,
                "total_tokens": 1,
                "total_price": 1,
                "currency": 1
            }
        },
        {
            "$group": {
                "_id": {
                    "date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$date"
                        }
                    },
                    "model_provider": "$model_provider",
                    "model_id": "$model_id"
                },
                "conversation_count": {"$sum": 1},
                "total_tokens": {"$sum": "$total_tokens"},
                "total_price": {"$sum": "$total_price"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "model_provider": "$_id.model_provider",
                "model_id": "$_id.model_id",
                "conversation_count": 1,
                "total_tokens": 1,
                "total_price": 1,
                "average_tokens_per_conversation": {
                    "$divide": ["$total_tokens", "$conversation_count"]
                },
                "average_price_per_conversation": {
                    "$divide": ["$total_price", "$conversation_count"]
                }
            }
        },
        {
            "$sort": {
                "date": 1,
                "model_provider": 1,
                "model_id": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_token_usage_metrics_view(client):
    """
    Create a view for token usage metrics.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "token_usage_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False
            }
        },
        {
            "$project": {
                "date": {
                    "$dateFromString": {
                        "dateString": "$created_at"
                    }
                },
                "app_id": 1,
                "model_id": 1,
                "total_tokens": 1,
                "total_price": 1,
                "message_count": 1,
                "system_instruction_tokens": 1
            }
        },
        {
            "$group": {
                "_id": {
                    "date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$date"
                        }
                    },
                    "app_id": "$app_id",
                    "model_id": "$model_id"
                },
                "conversation_count": {"$sum": 1},
                "total_tokens": {"$sum": "$total_tokens"},
                "total_price": {"$sum": "$total_price"},
                "total_messages": {"$sum": "$message_count"},
                "total_system_tokens": {"$sum": "$system_instruction_tokens"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "app_id": "$_id.app_id",
                "model_id": "$_id.model_id",
                "conversation_count": 1,
                "total_tokens": 1,
                "total_price": 1,
                "total_messages": 1,
                "total_system_tokens": 1,
                "average_tokens_per_conversation": {
                    "$divide": ["$total_tokens", "$conversation_count"]
                },
                "average_tokens_per_message": {
                    "$cond": [
                        {"$eq": ["$total_messages", 0]},
                        0,
                        {"$divide": ["$total_tokens", "$total_messages"]}
                    ]
                },
                "average_price_per_token": {
                    "$cond": [
                        {"$eq": ["$total_tokens", 0]},
                        0,
                        {"$divide": ["$total_price", "$total_tokens"]}
                    ]
                },
                "system_token_percentage": {
                    "$cond": [
                        {"$eq": ["$total_tokens", 0]},
                        0,
                        {
                            "$multiply": [
                                {"$divide": ["$total_system_tokens", "$total_tokens"]},
                                100
                            ]
                        }
                    ]
                }
            }
        },
        {
            "$sort": {
                "date": 1,
                "app_id": 1,
                "model_id": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)

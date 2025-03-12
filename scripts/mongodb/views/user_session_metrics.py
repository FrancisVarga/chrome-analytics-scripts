"""
Functions for creating user session metrics views.

This module contains functions for creating MongoDB views related to user
session data like language, currency, region etc.
"""

import logging
from analytics_framework.config import MONGODB_CONVERSATIONS_COLLECTION
from scripts.mongodb.utils import create_view

logger = logging.getLogger(__name__)


def create_user_session_metrics_view(client):
    """
    Create a view for user session metrics aggregating by language, currency and region.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "user_session_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "inputs": {"$exists": True},
                "inputs.username": {"$exists": True}
            }
        },
        {
            "$project": {
                "date": {
                    "$dateFromString": {
                        "dateString": "$created_at"
                    }
                },
                "username": "$inputs.username",
                "language": "$inputs.lang",
                "currency": "$inputs.currency",
                "region": "$inputs.rGroup",
                "origin": "$inputs.origin",
                "balance": {
                    "$convert": {
                        "input": "$inputs.balance",
                        "to": "double",
                        "onError": 0.0
                    }
                },
                "rewards_points": {
                    "$convert": {
                        "input": "$inputs.rewardsPoints",
                        "to": "double", 
                        "onError": 0.0
                    }
                }
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
                    "language": "$language",
                    "currency": "$currency", 
                    "region": "$region"
                },
                "unique_users": {"$addToSet": "$username"},
                "session_count": {"$sum": 1},
                "origins": {"$addToSet": "$origin"},
                "total_balance": {"$sum": "$balance"},
                "total_rewards": {"$sum": "$rewards_points"},
                "avg_balance": {"$avg": "$balance"},
                "avg_rewards": {"$avg": "$rewards_points"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "language": "$_id.language",
                "currency": "$_id.currency",
                "region": "$_id.region",
                "unique_user_count": {"$size": "$unique_users"},
                "session_count": 1,
                "origin_count": {"$size": "$origins"},
                "total_balance": 1,
                "total_rewards": 1,
                "avg_balance": 1,
                "avg_rewards": 1,
                "origins": 1
            }
        },
        {
            "$sort": {
                "date": 1,
                "unique_user_count": -1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)
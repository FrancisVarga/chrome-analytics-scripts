"""
Functions for creating category metrics views.

This module contains functions for creating MongoDB views related to category
distribution and metrics.
"""

import logging
from analytics_framework.config import MONGODB_CONVERSATIONS_COLLECTION
from scripts.mongodb.utils import create_view

logger = logging.getLogger(__name__)


def create_category_distribution_view(client):
    """
    Create a view for category distribution.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "category_distribution"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False,
                "categories": {"$exists": True, "$ne": []}
            }
        },
        {
            "$unwind": "$categories"
        },
        {
            "$project": {
                "date": {
                    "$dateFromString": {
                        "dateString": "$created_at"
                    }
                },
                "app_id": 1,
                "category_type": "$categories.category_type",
                "category_value": "$categories.category_value",
                "confidence_score": "$categories.confidence_score"
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
                    "category_type": "$category_type",
                    "category_value": "$category_value"
                },
                "conversation_count": {"$sum": 1},
                "average_confidence": {"$avg": "$confidence_score"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "app_id": "$_id.app_id",
                "category_type": "$_id.category_type",
                "category_value": "$_id.category_value",
                "conversation_count": 1,
                "average_confidence": 1
            }
        },
        {
            "$sort": {
                "date": 1,
                "app_id": 1,
                "category_type": 1,
                "conversation_count": -1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)

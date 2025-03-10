"""
Functions for creating agent metrics views.

This module contains functions for creating MongoDB views related to 
agent metrics, specifically counting messages containing "#AGENT".
"""

import logging
from analytics_framework.config import MONGODB_CONVERSATIONS_COLLECTION
from scripts.mongodb.utils import create_view

logger = logging.getLogger(__name__)


def create_agent_mention_metrics_view(client):
    """
    Create a view that counts messages containing "#AGENT" text.
    
    This view aggregates conversations and counts messages that contain
    the "#AGENT" text string, providing metrics by date, app_id, and model_id.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "agent_mention_metrics"
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
                "from_end_user_id": 1,
                "messages": 1
            }
        },
        {
            "$unwind": "$messages"
        },
        {
            "$match": {
                "messages.answer": {"$regex": "#AGENT", "$options": "i"}
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
                "agent_mention_count": {"$sum": 1},
                "unique_conversations": {"$addToSet": "$_id"},
                "unique_users": {"$addToSet": "$from_end_user_id"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "app_id": "$_id.app_id",
                "model_id": "$_id.model_id",
                "agent_mention_count": 1,
                "unique_conversation_count": {"$size": "$unique_conversations"},
                "unique_user_count": {"$size": "$unique_users"}
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


def create_agent_mention_distribution_view(client):
    """
    Create a view that shows the distribution of "#AGENT" mentions across conversations.
    
    This view provides metrics on how many "#AGENT" mentions appear in each conversation,
    categorizing conversations by the number of agent mentions they contain.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "agent_mention_distribution"
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
                "messages": 1
            }
        },
        {
            "$addFields": {
                "agent_mentions": {
                    "$size": {
                        "$filter": {
                            "input": "$messages",
                            "as": "message",
                            "cond": {
                                "$regexMatch": {
                                    "input": "$$message.answer",
                                    "regex": "#AGENT",
                                    "options": "i"
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "$match": {
                "agent_mentions": {"$gt": 0}
            }
        },
        {
            "$addFields": {
                "mention_bucket": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$agent_mentions", 1]}, "then": "1"},
                            {"case": {"$lte": ["$agent_mentions", 3]}, "then": "2-3"},
                            {"case": {"$lte": ["$agent_mentions", 5]}, "then": "4-5"},
                            {"case": {"$lte": ["$agent_mentions", 10]}, "then": "6-10"}
                        ],
                        "default": "11+"
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
                    "app_id": "$app_id",
                    "mention_bucket": "$mention_bucket"
                },
                "conversation_count": {"$sum": 1},
                "total_mentions": {"$sum": "$agent_mentions"},
                "avg_mentions_per_conversation": {"$avg": "$agent_mentions"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "app_id": "$_id.app_id",
                "mention_bucket": "$_id.mention_bucket",
                "conversation_count": 1,
                "total_mentions": 1,
                "avg_mentions_per_conversation": 1
            }
        },
        {
            "$sort": {
                "date": 1,
                "app_id": 1,
                "mention_bucket": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)

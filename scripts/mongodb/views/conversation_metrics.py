"""
Functions for creating conversation metrics views.

This module contains functions for creating MongoDB views related to 
conversation metrics.
"""

import logging
from analytics_framework.config import MONGODB_CONVERSATIONS_COLLECTION
from scripts.mongodb.utils import create_view

logger = logging.getLogger(__name__)


def create_weekly_conversation_metrics_view(client):
    """
    Create a view for weekly conversation metrics.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "weekly_conversation_metrics"
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
                "message_count": 1,
                "total_tokens": 1,
                "total_price": 1,
                "currency": 1
            }
        },
        {
            "$group": {
                "_id": {
                    "week": {
                        "$dateToString": {
                            "format": "%G-W%V",  # ISO week year and week number
                            "date": "$date"
                        }
                    },
                    "app_id": "$app_id"
                },
                "conversation_count": {"$sum": 1},
                "total_messages": {"$sum": "$message_count"},
                "total_tokens": {"$sum": "$total_tokens"},
                "total_price": {"$sum": "$total_price"},
                "unique_users": {"$addToSet": "$from_end_user_id"},
                "start_date": {"$min": "$date"},
                "end_date": {"$max": "$date"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "week": "$_id.week",
                "app_id": "$_id.app_id",
                "start_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$start_date"
                    }
                },
                "end_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$end_date"
                    }
                },
                "conversation_count": 1,
                "total_messages": 1,
                "total_tokens": 1,
                "total_price": 1,
                "unique_user_count": {"$size": "$unique_users"}
            }
        },
        {
            "$sort": {
                "week": 1,
                "app_id": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_monthly_conversation_metrics_view(client):
    """
    Create a view for monthly conversation metrics.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "monthly_conversation_metrics"
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
                "message_count": 1,
                "total_tokens": 1,
                "total_price": 1,
                "currency": 1
            }
        },
        {
            "$group": {
                "_id": {
                    "month": {
                        "$dateToString": {
                            "format": "%Y-%m",
                            "date": "$date"
                        }
                    },
                    "app_id": "$app_id"
                },
                "conversation_count": {"$sum": 1},
                "total_messages": {"$sum": "$message_count"},
                "total_tokens": {"$sum": "$total_tokens"},
                "total_price": {"$sum": "$total_price"},
                "unique_users": {"$addToSet": "$from_end_user_id"},
                "start_date": {"$min": "$date"},
                "end_date": {"$max": "$date"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "month": "$_id.month",
                "app_id": "$_id.app_id",
                "start_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$start_date"
                    }
                },
                "end_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$end_date"
                    }
                },
                "conversation_count": 1,
                "total_messages": 1,
                "total_tokens": 1,
                "total_price": 1,
                "unique_user_count": {"$size": "$unique_users"}
            }
        },
        {
            "$sort": {
                "month": 1,
                "app_id": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_daily_conversation_metrics_view(client):
    """
    Create a view for daily conversation metrics.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "daily_conversation_metrics"
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
                "message_count": 1,
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
                    "app_id": "$app_id"
                },
                "conversation_count": {"$sum": 1},
                "total_messages": {"$sum": "$message_count"},
                "total_tokens": {"$sum": "$total_tokens"},
                "total_price": {"$sum": "$total_price"},
                "unique_users": {"$addToSet": "$from_end_user_id"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "app_id": "$_id.app_id",
                "conversation_count": 1,
                "total_messages": 1,
                "total_tokens": 1,
                "total_price": 1,
                "unique_user_count": {"$size": "$unique_users"}
            }
        },
        {
            "$sort": {
                "date": 1,
                "app_id": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_conversation_length_distribution_view(client):
    """
    Create a view for conversation length distribution.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "conversation_length_distribution"
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
                "message_count": 1,
                "length_bucket": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lte": ["$message_count", 2]}, 
                             "then": "1-2"},
                            {"case": {"$lte": ["$message_count", 5]}, 
                             "then": "3-5"},
                            {"case": {"$lte": ["$message_count", 10]}, 
                             "then": "6-10"},
                            {"case": {"$lte": ["$message_count", 20]}, 
                             "then": "11-20"}
                        ],
                        "default": "21+"
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
                    "length_bucket": "$length_bucket"
                },
                "conversation_count": {"$sum": 1},
                "average_message_count": {"$avg": "$message_count"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "app_id": "$_id.app_id",
                "length_bucket": "$_id.length_bucket",
                "conversation_count": 1,
                "average_message_count": 1
            }
        },
        {
            "$sort": {
                "date": 1,
                "app_id": 1,
                "length_bucket": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)

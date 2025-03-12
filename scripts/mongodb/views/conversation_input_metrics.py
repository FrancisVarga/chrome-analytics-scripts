"""
Functions for creating conversation input metrics views.

This module contains functions for creating MongoDB views related to 
conversation input metrics, analyzing the inputs field in conversations.
"""

import logging
from analytics_framework.config import MONGODB_CONVERSATIONS_COLLECTION
from scripts.mongodb.utils import create_view

logger = logging.getLogger(__name__)


def create_conversation_input_metrics_view(client):
    """
    Create a view for conversation input metrics.
    This view analyzes the inputs field in conversations to provide metrics
    on different input types and their usage patterns.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "conversation_input_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False,
                "inputs": {"$exists": True, "$ne": {}}
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
                "inputs": 1,
                "message_count": 1,
                "total_tokens": 1,
                "total_price": 1
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
                "unique_users": {"$addToSet": "$from_end_user_id"},
                "input_samples": {"$push": "$inputs"}
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
                "unique_user_count": {"$size": "$unique_users"},
                "input_samples": {"$slice": ["$input_samples", 10]}  # Limit to 10 samples
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


def create_conversation_input_field_distribution_view(client):
    """
    Create a view for conversation input field distribution.
    This view analyzes which fields are present in the inputs object
    and their distribution across conversations.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "conversation_input_field_distribution"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False,
                "inputs": {"$exists": True, "$ne": {}}
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
                "input_fields": {"$objectToArray": "$inputs"}
            }
        },
        {
            "$unwind": "$input_fields"
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
                    "field_name": "$input_fields.k"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "app_id": "$_id.app_id",
                "field_name": "$_id.field_name",
                "count": 1
            }
        },
        {
            "$sort": {
                "date": 1,
                "app_id": 1,
                "count": -1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_conversation_input_type_metrics_view(client):
    """
    Create a view for conversation input type metrics.
    This view analyzes the types of values in the inputs field
    (string, number, array, object, etc.) and their distribution.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "conversation_input_type_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False,
                "inputs": {"$exists": True, "$ne": {}}
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
                "input_fields": {"$objectToArray": "$inputs"}
            }
        },
        {
            "$unwind": "$input_fields"
        },
        {
            "$project": {
                "date": 1,
                "app_id": 1,
                "field_name": "$input_fields.k",
                "value_type": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": [{"$type": "$input_fields.v"}, "string"]}, "then": "string"},
                            {"case": {"$eq": [{"$type": "$input_fields.v"}, "number"]}, "then": "number"},
                            {"case": {"$eq": [{"$type": "$input_fields.v"}, "bool"]}, "then": "boolean"},
                            {"case": {"$eq": [{"$type": "$input_fields.v"}, "array"]}, "then": "array"},
                            {"case": {"$eq": [{"$type": "$input_fields.v"}, "object"]}, "then": "object"},
                            {"case": {"$eq": [{"$type": "$input_fields.v"}, "null"]}, "then": "null"}
                        ],
                        "default": "other"
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "date": "$date",
                    "app_id": "$app_id",
                    "field_name": "$field_name",
                    "value_type": "$value_type"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$_id.date"
                    }
                },
                "app_id": "$_id.app_id",
                "field_name": "$_id.field_name",
                "value_type": "$_id.value_type",
                "count": 1
            }
        },
        {
            "$sort": {
                "date": 1,
                "app_id": 1,
                "field_name": 1,
                "count": -1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_conversation_input_list_metrics_view(client):
    """
    Create a view for conversation input list metrics.
    This view specifically analyzes list-type fields in the inputs
    (like listDeposit, listWithdrawal, etc.) to provide metrics on their usage.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "conversation_input_list_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False,
                "$or": [
                    {"inputs.listDeposit": {"$exists": True}},
                    {"inputs.listWithdrawal": {"$exists": True}},
                    {"inputs.listDepositMethods": {"$exists": True}},
                    {"inputs.listWithdrawalMethods": {"$exists": True}},
                    {"inputs.banks": {"$exists": True}}
                ]
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
                "listDeposit": {
                    "$cond": [
                        {"$isArray": "$inputs.listDeposit"},
                        {"$size": "$inputs.listDeposit"},
                        0
                    ]
                },
                "listWithdrawal": {
                    "$cond": [
                        {"$isArray": "$inputs.listWithdrawal"},
                        {"$size": "$inputs.listWithdrawal"},
                        0
                    ]
                },
                "listDepositMethods": {
                    "$cond": [
                        {"$isArray": "$inputs.listDepositMethods"},
                        {"$size": "$inputs.listDepositMethods"},
                        0
                    ]
                },
                "listWithdrawalMethods": {
                    "$cond": [
                        {"$isArray": "$inputs.listWithdrawalMethods"},
                        {"$size": "$inputs.listWithdrawalMethods"},
                        0
                    ]
                },
                "banks": {
                    "$cond": [
                        {"$isArray": "$inputs.banks"},
                        {"$size": "$inputs.banks"},
                        0
                    ]
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
                    "app_id": "$app_id"
                },
                "conversation_count": {"$sum": 1},
                "unique_users": {"$addToSet": "$from_end_user_id"},
                "avg_listDeposit_size": {"$avg": "$listDeposit"},
                "max_listDeposit_size": {"$max": "$listDeposit"},
                "avg_listWithdrawal_size": {"$avg": "$listWithdrawal"},
                "max_listWithdrawal_size": {"$max": "$listWithdrawal"},
                "avg_listDepositMethods_size": {"$avg": "$listDepositMethods"},
                "max_listDepositMethods_size": {"$max": "$listDepositMethods"},
                "avg_listWithdrawalMethods_size": {"$avg": "$listWithdrawalMethods"},
                "max_listWithdrawalMethods_size": {"$max": "$listWithdrawalMethods"},
                "avg_banks_size": {"$avg": "$banks"},
                "max_banks_size": {"$max": "$banks"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "app_id": "$_id.app_id",
                "conversation_count": 1,
                "unique_user_count": {"$size": "$unique_users"},
                "avg_listDeposit_size": {"$round": ["$avg_listDeposit_size", 2]},
                "max_listDeposit_size": 1,
                "avg_listWithdrawal_size": {"$round": ["$avg_listWithdrawal_size", 2]},
                "max_listWithdrawal_size": 1,
                "avg_listDepositMethods_size": {"$round": ["$avg_listDepositMethods_size", 2]},
                "max_listDepositMethods_size": 1,
                "avg_listWithdrawalMethods_size": {"$round": ["$avg_listWithdrawalMethods_size", 2]},
                "max_listWithdrawalMethods_size": 1,
                "avg_banks_size": {"$round": ["$avg_banks_size", 2]},
                "max_banks_size": 1
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


def create_conversation_input_grouping_metrics_view(client):
    """
    Create a view for conversation metrics grouped by input.lang, input.currency, and input.rGroup.
    This view analyzes conversations per day based on these specific input fields.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "conversation_input_grouping_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False,
                "inputs": {"$exists": True}
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
                "language": {"$ifNull": ["$inputs.lang", "unknown"]},
                "currency": {"$ifNull": ["$inputs.currency", "unknown"]},
                "risk_group": {"$ifNull": ["$inputs.rGroup", "unknown"]},
                "message_count": 1,
                "total_tokens": 1,
                "total_price": 1,
                "messages": 1
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
                    "risk_group": "$risk_group"
                },
                "conversation_count": {"$sum": 1},
                "total_messages": {"$sum": "$message_count"},
                "total_tokens": {"$sum": "$total_tokens"},
                "total_price": {"$sum": "$total_price"},
                "unique_users": {"$addToSet": "$from_end_user_id"},
                "agent_mentions": {
                    "$sum": {
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
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "language": "$_id.language",
                "currency": "$_id.currency",
                "risk_group": "$_id.risk_group",
                "conversation_count": 1,
                "total_messages": 1,
                "total_tokens": 1,
                "total_price": 1,
                "unique_user_count": {"$size": "$unique_users"},
                "avg_messages_per_conversation": {
                    "$cond": [
                        {"$eq": ["$conversation_count", 0]},
                        0,
                        {"$divide": ["$total_messages", "$conversation_count"]}
                    ]
                },
                "agent_mention_count": "$agent_mentions",
                "avg_agent_mentions_per_conversation": {
                    "$cond": [
                        {"$eq": ["$conversation_count", 0]},
                        0,
                        {"$divide": ["$agent_mentions", "$conversation_count"]}
                    ]
                }
            }
        },
        {
            "$sort": {
                "date": 1,
                "language": 1,
                "currency": 1,
                "risk_group": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_conversation_count_by_input_fields_view(client):
    """
    Create a view that counts conversations based on rGroup, lang, and currency per day.
    This view provides a simple count of conversations for each combination of these fields.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "conversation_count_by_input_fields"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False,
                "inputs": {"$exists": True}
            }
        },
        {
            "$project": {
                "date": {
                    "$dateFromString": {
                        "dateString": "$created_at"
                    }
                },
                "risk_group": {"$ifNull": ["$inputs.rGroup", "unknown"]},
                "language": {"$ifNull": ["$inputs.lang", "unknown"]},
                "currency": {"$ifNull": ["$inputs.currency", "unknown"]}
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
                    "risk_group": "$risk_group",
                    "language": "$language",
                    "currency": "$currency"
                },
                "conversation_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "risk_group": "$_id.risk_group",
                "language": "$_id.language",
                "currency": "$_id.currency",
                "conversation_count": 1
            }
        },
        {
            "$sort": {
                "date": 1,
                "risk_group": 1,
                "language": 1,
                "currency": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_daily_conversations_and_messages_by_input_fields_view(client):
    """
    Create a view that shows daily conversations and messages based on input.lang,
    input.currency, and input.rGroup. This view provides detailed daily metrics
    for each combination of these fields.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "daily_conversations_and_messages_by_input_fields"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "is_deleted": False,
                "inputs": {"$exists": True}
            }
        },
        {
            "$project": {
                "date": {
                    "$dateFromString": {
                        "dateString": "$created_at"
                    }
                },
                "risk_group": {"$ifNull": ["$inputs.rGroup", "unknown"]},
                "language": {"$ifNull": ["$inputs.lang", "unknown"]},
                "currency": {"$ifNull": ["$inputs.currency", "unknown"]},
                "message_count": 1,
                "total_tokens": 1,
                "total_price": 1,
                "from_end_user_id": 1
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
                    "risk_group": "$risk_group",
                    "language": "$language",
                    "currency": "$currency"
                },
                "conversation_count": {"$sum": 1},
                "total_messages": {"$sum": "$message_count"},
                "total_tokens": {"$sum": "$total_tokens"},
                "total_price": {"$sum": "$total_price"},
                "unique_users": {"$addToSet": "$from_end_user_id"},
                "avg_messages_per_conversation": {
                    "$avg": "$message_count"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "risk_group": "$_id.risk_group",
                "language": "$_id.language",
                "currency": "$_id.currency",
                "conversation_count": 1,
                "total_messages": 1,
                "total_tokens": 1,
                "total_price": 1,
                "unique_user_count": {"$size": "$unique_users"},
                "avg_messages_per_conversation": {"$round": ["$avg_messages_per_conversation", 2]}
            }
        },
        {
            "$sort": {
                "date": 1,
                "risk_group": 1,
                "language": 1,
                "currency": 1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)

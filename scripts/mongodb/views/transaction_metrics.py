"""
Functions for creating transaction metrics views.

This module contains functions for creating MongoDB views related to
financial transactions (deposits and withdrawals).
"""

import logging
from analytics_framework.config import MONGODB_CONVERSATIONS_COLLECTION
from scripts.mongodb.utils import create_view

logger = logging.getLogger(__name__)


def create_deposit_metrics_view(client):
    """
    Create a view for deposit transaction metrics.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "deposit_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "inputs": {"$exists": True},
                "inputs.listDeposit": {"$exists": True, "$ne": "[]"}
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
                "currency": "$inputs.currency",
                "region": "$inputs.rGroup",
                "deposits": {
                    "$function": {
                        "body": """
                            function(depositStr) {
                                try {
                                    return JSON.parse(depositStr);
                                } catch (e) {
                                    return [];
                                }
                            }
                        """,
                        "args": ["$inputs.listDeposit"],
                        "lang": "js"
                    }
                }
            }
        },
        {
            "$unwind": "$deposits"
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
                    "currency": "$currency",
                    "region": "$region",
                    "status": "$deposits.status"
                },
                "unique_users": {"$addToSet": "$username"},
                "deposit_count": {"$sum": 1},
                "deposit_ids": {"$addToSet": "$deposits.id"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "currency": "$_id.currency",
                "region": "$_id.region",
                "status": "$_id.status",
                "unique_user_count": {"$size": "$unique_users"},
                "deposit_count": 1,
                "unique_deposit_count": {"$size": "$deposit_ids"}
            }
        },
        {
            "$sort": {
                "date": 1,
                "deposit_count": -1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_payment_method_metrics_view(client):
    """
    Create a view for payment method usage metrics.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "payment_method_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "inputs": {"$exists": True},
                "$or": [
                    {"inputs.listDepositMethods": {"$exists": True, "$ne": "[]"}},
                    {"inputs.listWithdrawalMethods": {"$exists": True, "$ne": "[]"}}
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
                "username": "$inputs.username",
                "currency": "$inputs.currency",
                "region": "$inputs.rGroup",
                "deposit_methods": {
                    "$function": {
                        "body": """
                            function(methodStr) {
                                try {
                                    return JSON.parse(methodStr);
                                } catch (e) {
                                    return [];
                                }
                            }
                        """,
                        "args": ["$inputs.listDepositMethods"],
                        "lang": "js"
                    }
                },
                "withdrawal_methods": {
                    "$function": {
                        "body": """
                            function(methodStr) {
                                try {
                                    return JSON.parse(methodStr);
                                } catch (e) {
                                    return [];
                                }
                            }
                        """,
                        "args": ["$inputs.listWithdrawalMethods"],
                        "lang": "js"
                    }
                }
            }
        },
        {
            "$facet": {
                "deposit_methods": [
                    {"$unwind": "$deposit_methods"},
                    {
                        "$group": {
                            "_id": {
                                "date": "$date",
                                "group": "$deposit_methods.group",
                                "name": "$deposit_methods.name"
                            },
                            "method_count": {"$sum": 1},
                            "unique_users": {"$addToSet": "$username"}
                        }
                    }
                ],
                "withdrawal_methods": [
                    {"$unwind": "$withdrawal_methods"},
                    {
                        "$group": {
                            "_id": {
                                "date": "$date",
                                "group": "$withdrawal_methods.group",
                                "name": "$withdrawal_methods.name"
                            },
                            "method_count": {"$sum": 1},
                            "unique_users": {"$addToSet": "$username"}
                        }
                    }
                ]
            }
        },
        {
            "$project": {
                "deposit_methods": {
                    "$map": {
                        "input": "$deposit_methods",
                        "as": "method",
                        "in": {
                            "date": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$$method._id.date"
                                }
                            },
                            "type": "deposit",
                            "group": "$$method._id.group",
                            "name": "$$method._id.name",
                            "usage_count": "$$method.method_count",
                            "unique_user_count": {"$size": "$$method.unique_users"}
                        }
                    }
                },
                "withdrawal_methods": {
                    "$map": {
                        "input": "$withdrawal_methods",
                        "as": "method",
                        "in": {
                            "date": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$$method._id.date"
                                }
                            },
                            "type": "withdrawal",
                            "group": "$$method._id.group",
                            "name": "$$method._id.name",
                            "usage_count": "$$method.method_count",
                            "unique_user_count": {"$size": "$$method.unique_users"}
                        }
                    }
                }
            }
        },
        {
            "$project": {
                "methods": {
                    "$concatArrays": ["$deposit_methods", "$withdrawal_methods"]
                }
            }
        },
        {
            "$unwind": "$methods"
        },
        {
            "$replaceRoot": {"newRoot": "$methods"}
        },
        {
            "$sort": {
                "date": 1,
                "type": 1,
                "usage_count": -1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)


def create_bank_usage_metrics_view(client):
    """
    Create a view for bank usage metrics.
    
    Args:
        client: MongoDB client
        
    Returns:
        bool: True if successful, False otherwise
    """
    view_name = "bank_usage_metrics"
    logger.info(f"Creating view: {view_name}...")
    
    pipeline = [
        {
            "$match": {
                "inputs": {"$exists": True},
                "inputs.banks": {"$exists": True, "$ne": "[]"}
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
                "currency": "$inputs.currency",
                "region": "$inputs.rGroup",
                "banks": {
                    "$function": {
                        "body": """
                            function(banksStr) {
                                try {
                                    return JSON.parse(banksStr);
                                } catch (e) {
                                    return [];
                                }
                            }
                        """,
                        "args": ["$inputs.banks"],
                        "lang": "js"
                    }
                }
            }
        },
        {
            "$unwind": "$banks"
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
                    "bank_name": "$banks.bankName"
                },
                "unique_users": {"$addToSet": "$username"},
                "display_names": {"$addToSet": "$banks.displayName"},
                "usage_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "bank_name": "$_id.bank_name",
                "unique_user_count": {"$size": "$unique_users"},
                "unique_account_count": {"$size": "$display_names"},
                "usage_count": 1
            }
        },
        {
            "$sort": {
                "date": 1,
                "usage_count": -1
            }
        }
    ]
    
    return create_view(client, view_name, MONGODB_CONVERSATIONS_COLLECTION, pipeline)
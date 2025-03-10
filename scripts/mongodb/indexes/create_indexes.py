"""
Functions for creating MongoDB indexes.

This module contains functions for creating indexes on MongoDB collections.
"""

import logging
from analytics_framework.models.mongodb_schema import (
    CONVERSATION_ANALYTICS_INDEXES,
    CONVERSATION_MESSAGES_INDEXES,
    CONVERSATION_CATEGORIES_INDEXES,
    CONVERSATION_TRANSLATIONS_INDEXES,
    ANALYTICS_REPORTS_INDEXES,
    USER_ANALYTICS_INDEXES
)
from analytics_framework.config import (
    MONGODB_CONVERSATIONS_COLLECTION,
    MONGODB_MESSAGES_COLLECTION,
    MONGODB_CATEGORIES_COLLECTION,
    MONGODB_TRANSLATIONS_COLLECTION,
    MONGODB_ANALYTICS_REPORTS_COLLECTION,
    MONGODB_USER_ANALYTICS_COLLECTION
)

logger = logging.getLogger(__name__)


def create_collection_indexes(client, collection_name, indexes):
    """
    Create indexes for a specific collection.
    
    Args:
        client: MongoDB client
        collection_name: Name of the collection
        indexes: List of index definitions
        
    Returns:
        int: Number of indexes created successfully
    """
    logger.info(f"Creating indexes for {collection_name} collection...")
    success_count = 0
    
    for index in indexes:
        try:
            client.base_client.create_index(
                collection_name,
                list(index["key"].items())
            )
            logger.info(f"Created index: {index['key']}")
            success_count += 1
        except Exception as e:
            logger.error(f"Error creating index {index['key']}: {str(e)}")
    
    return success_count


def create_all_indexes(client):
    """
    Create indexes for all collections.
    
    Args:
        client: MongoDB client
        
    Returns:
        dict: Dictionary with collection names as keys and number of 
              successfully created indexes as values
    """
    logger.info("Creating indexes for all collections...")
    
    results = {}
    
    # Create indexes for conversations collection
    results[MONGODB_CONVERSATIONS_COLLECTION] = create_collection_indexes(
        client, 
        MONGODB_CONVERSATIONS_COLLECTION, 
        CONVERSATION_ANALYTICS_INDEXES
    )
    
    # Create indexes for messages collection
    results[MONGODB_MESSAGES_COLLECTION] = create_collection_indexes(
        client, 
        MONGODB_MESSAGES_COLLECTION, 
        CONVERSATION_MESSAGES_INDEXES
    )
    
    # Create indexes for categories collection
    results[MONGODB_CATEGORIES_COLLECTION] = create_collection_indexes(
        client, 
        MONGODB_CATEGORIES_COLLECTION, 
        CONVERSATION_CATEGORIES_INDEXES
    )
    
    # Create indexes for translations collection
    results[MONGODB_TRANSLATIONS_COLLECTION] = create_collection_indexes(
        client, 
        MONGODB_TRANSLATIONS_COLLECTION, 
        CONVERSATION_TRANSLATIONS_INDEXES
    )
    
    # Create indexes for analytics reports collection
    results[MONGODB_ANALYTICS_REPORTS_COLLECTION] = create_collection_indexes(
        client, 
        MONGODB_ANALYTICS_REPORTS_COLLECTION, 
        ANALYTICS_REPORTS_INDEXES
    )
    
    # Create indexes for user analytics collection
    results[MONGODB_USER_ANALYTICS_COLLECTION] = create_collection_indexes(
        client, 
        MONGODB_USER_ANALYTICS_COLLECTION, 
        USER_ANALYTICS_INDEXES
    )
    
    logger.info("All indexes created successfully.")
    return results

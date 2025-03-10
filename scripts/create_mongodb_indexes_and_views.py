#!/usr/bin/env python
"""
Script to create MongoDB indexes and views with aggregators.

This script creates:
1. Indexes for all collections as defined in the MongoDB schema
2. Views with aggregators for analytics purposes

Usage:
    python -m scripts.create_mongodb_indexes_and_views
"""

import logging
import sys
import os
from analytics_framework.storage.mongodb.client import MongoDBClient
from scripts.mongodb.indexes import create_all_indexes
from scripts.mongodb.views import VIEW_CREATORS

# Add the parent directory to the path
sys.path.insert(
    0, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/mongodb_setup.log')
    ]
)

logger = logging.getLogger(__name__)


def create_views(client):
    """
    Create all views with aggregators for analytics purposes.
    
    Args:
        client: MongoDB client
        
    Returns:
        dict: Dictionary with view names as keys and success status as values
    """
    logger.info("Creating views with aggregators...")
    
    results = {}
    
    # Create all views using the view creators from the views module
    for view_creator in VIEW_CREATORS:
        view_name = view_creator.__name__.replace('create_', '')
        view_name = view_name.replace('_view', '')
        results[view_name] = view_creator(client)
    
    logger.info("Views creation completed.")
    return results


def main():
    """Main function to create indexes and views."""
    logger.info("Starting MongoDB indexes and views creation...")
    
    try:
        # Create MongoDB client
        client = MongoDBClient()
        
        # Check if MongoDB connection is alive
        if not client.ping():
            logger.error(
                "MongoDB connection failed. "
                "Please check your MongoDB configuration."
            )
            return
        
        # Create indexes
        index_results = create_all_indexes(client)
        logger.info(f"Index creation results: {index_results}")
        
        # Create views
        view_results = create_views(client)
        logger.info(f"View creation results: {view_results}")
        
        # Close MongoDB connection
        client.close()
        
        logger.info(
            "MongoDB indexes and views creation completed successfully."
        )
    except Exception as e:
        logger.error(f"Error creating MongoDB indexes and views: {str(e)}")


if __name__ == "__main__":
    main()

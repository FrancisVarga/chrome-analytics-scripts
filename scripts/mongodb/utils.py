"""
Utility functions for MongoDB operations.

This module contains common utility functions for MongoDB operations.
"""

import logging

logger = logging.getLogger(__name__)


def drop_view_if_exists(client, view_name):
    """
    Drop a view if it exists.
    
    Args:
        client: MongoDB client
        view_name: Name of the view to drop
    """
    try:
        # Drop the view if it exists
        client.base_client.db.command(
            "drop",
            view_name,
            writeConcern={"w": "majority", "wtimeout": 5000}
        )
        logger.info(f"Dropped existing view: {view_name}")
    except Exception:
        # View doesn't exist, which is fine
        pass


def create_view(client, view_name, collection, pipeline):
    """
    Create a MongoDB view.
    
    Args:
        client: MongoDB client
        view_name: Name of the view to create
        collection: Source collection for the view
        pipeline: Aggregation pipeline for the view
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Drop the view if it exists
        drop_view_if_exists(client, view_name)
        
        # Create the view
        client.base_client.db.command(
            "create",
            view_name,
            viewOn=collection,
            pipeline=pipeline,
            writeConcern={"w": "majority", "wtimeout": 5000}
        )
        logger.info(f"Created view: {view_name}")
        return True
    except Exception as e:
        logger.error(f"Error creating view {view_name}: {str(e)}")
        return False

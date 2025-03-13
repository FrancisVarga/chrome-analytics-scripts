#!/usr/bin/env python
"""
Script to sync conversations from MongoDB to NocoDB.

This script retrieves conversations from MongoDB and syncs them to NocoDB.
It supports incremental syncing by tracking the last synced conversation.
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Add parent directory to path to import from analytics_framework
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics_framework.storage.mongodb.client import MongoDBClient
from analytics_framework.api.nocodb_client import NocoDBClient
from analytics_framework.utils.processing_state import ProcessingState
from analytics_framework.config import (
    MONGODB_URI,
    MONGODB_DATABASE,
    MONGODB_CONVERSATIONS_COLLECTION,
    NOCODB_BASE_URL,
    NOCODB_API_TOKEN,
    NOCODB_PROJECT_ID,
    NOCODB_CONVERSATION_TABLE,
    NOCODB_MESSAGES_TABLE
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/sync_mongodb_to_nocodb.log')
    ]
)

logger = logging.getLogger(__name__)


class MongoDBToNocoDBSyncer:
    """Class to sync conversations from MongoDB to NocoDB."""
    
    def __init__(
        self,
        mongodb_uri: str = MONGODB_URI,
        mongodb_database: str = MONGODB_DATABASE,
        mongodb_collection: str = MONGODB_CONVERSATIONS_COLLECTION,
        nocodb_base_url: str = NOCODB_BASE_URL,
        nocodb_api_token: str = NOCODB_API_TOKEN,
        nocodb_project_id: str = NOCODB_PROJECT_ID,
        nocodb_conversation_table: str = NOCODB_CONVERSATION_TABLE,
        nocodb_messages_table: str = NOCODB_MESSAGES_TABLE,
        state_file: str = "sync_state.json",
        batch_size: int = 100
    ):
        """
        Initialize the syncer.
        
        Args:
            mongodb_uri: MongoDB connection URI
            mongodb_database: MongoDB database name
            mongodb_collection: MongoDB collection name for conversations
            nocodb_base_url: NocoDB base URL
            nocodb_api_token: NocoDB API token
            nocodb_project_id: NocoDB project ID
            nocodb_conversation_table: NocoDB conversation table name
            nocodb_messages_table: NocoDB messages table name
            state_file: File to store sync state
            batch_size: Number of conversations to process in each batch
        """
        self.mongodb_collection = mongodb_collection
        self.nocodb_conversation_table = nocodb_conversation_table
        self.nocodb_messages_table = nocodb_messages_table
        self.batch_size = batch_size
        
        # Initialize MongoDB client
        self.mongodb_client = MongoDBClient(
            uri=mongodb_uri,
            database=mongodb_database
        )
        
        # Initialize NocoDB client
        self.nocodb_client = NocoDBClient(
            base_url=nocodb_base_url,
            api_token=nocodb_api_token,
            project_id=nocodb_project_id
        )
        
        # Initialize processing state
        self.state = ProcessingState(state_file)
        
        # Load last sync state
        self.last_sync_time = self.state.get("last_sync_time")
        self.last_conversation_id = self.state.get("last_conversation_id")
        
        logger.info(f"Initialized syncer with last sync time: {self.last_sync_time}")
    
    def _map_mongodb_to_nocodb_conversation(self, conversation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map MongoDB conversation to NocoDB format.
        
        Args:
            conversation: MongoDB conversation document
            
        Returns:
            NocoDB formatted conversation
        """
        # Create a copy to avoid modifying the original
        nocodb_conversation = conversation.copy()
        
        # Map _id to id if needed
        if "_id" in nocodb_conversation and "id" not in nocodb_conversation:
            nocodb_conversation["id"] = nocodb_conversation.pop("_id")
        
        # Remove MongoDB-specific fields that shouldn't be in NocoDB
        fields_to_remove = [
            "categories",  # MongoDB-specific categorization
            "messages",    # Messages are stored separately in NocoDB
            "message_count",  # Derived field
            "total_tokens",   # Derived field
            "total_price"     # Derived field
        ]
        
        for field in fields_to_remove:
            if field in nocodb_conversation:
                nocodb_conversation.pop(field)
        
        return nocodb_conversation
    
    def _map_mongodb_to_nocodb_message(self, message: Dict[str, Any], conversation_id: str) -> Dict[str, Any]:
        """
        Map MongoDB message to NocoDB format.
        
        Args:
            message: MongoDB message document
            conversation_id: Conversation ID
            
        Returns:
            NocoDB formatted message
        """
        # Create a copy to avoid modifying the original
        nocodb_message = message.copy()
        
        # Ensure conversation_id is set
        nocodb_message["conversation_id"] = conversation_id
        
        # Map message_id to id if needed
        if "message_id" in nocodb_message and "id" not in nocodb_message:
            nocodb_message["id"] = nocodb_message.pop("message_id")
        
        return nocodb_message
    
    def _get_conversations_from_mongodb(
        self,
        start_time: Optional[str] = None,
        start_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get conversations from MongoDB with pagination.
        
        Args:
            start_time: Start time for filtering (ISO format)
            start_id: Start conversation ID for pagination
            limit: Maximum number of conversations to return
            
        Returns:
            List of conversations
        """
        query = {}
        
        # Filter by time if provided
        if start_time:
            query["created_at"] = {"$gt": start_time}
        
        # Filter by ID if provided (for pagination)
        if start_id:
            query["_id"] = {"$gt": start_id}
        
        # Get conversations sorted by ID
        return self.mongodb_client.base_client.find(
            self.mongodb_collection,
            query=query,
            sort=[("_id", 1)],
            limit=limit
        )
    
    def _conversation_exists_in_nocodb(self, conversation_id: str) -> bool:
        """
        Check if a conversation exists in NocoDB.
        
        Args:
            conversation_id: Conversation ID
            
        Returns:
            True if the conversation exists, False otherwise
        """
        try:
            response = self.nocodb_client.fetch_records(
                table_name=self.nocodb_conversation_table,
                where=f"(id,eq,{conversation_id})"
            )
            
            return response.get("list") and len(response["list"]) > 0
        except Exception as e:
            logger.error(f"Error checking if conversation {conversation_id} exists: {str(e)}")
            return False
    
    def _sync_conversation(self, conversation: Dict[str, Any]) -> bool:
        """
        Sync a single conversation from MongoDB to NocoDB.
        
        Args:
            conversation: MongoDB conversation document
            
        Returns:
            True if successful, False otherwise
        """
        conversation_id = conversation.get("_id") or conversation.get("id")
        
        if not conversation_id:
            logger.error("Conversation missing ID, skipping")
            return False
        
        try:
            # Check if conversation already exists in NocoDB
            exists = self._conversation_exists_in_nocodb(conversation_id)
            
            # Map conversation to NocoDB format
            nocodb_conversation = self._map_mongodb_to_nocodb_conversation(conversation)
            
            # Create or update conversation in NocoDB
            # Format the request as { conversation_id, data } where data is the entire conversation object
            if exists:
                logger.debug(f"Updating conversation {conversation_id} in NocoDB")
                request_data = {
                    "conversation_id": conversation_id,
                    "data": nocodb_conversation
                }
                self.nocodb_client.update_record(
                    table_name=self.nocodb_conversation_table,
                    record_id=conversation_id,
                    updates=request_data
                )
            else:
                logger.debug(f"Creating conversation {conversation_id} in NocoDB")
                request_data = {
                    "conversation_id": conversation_id,
                    "data": nocodb_conversation
                }
                self.nocodb_client.create_record(
                    table_name=self.nocodb_conversation_table,
                    record=request_data
                )
            
            # Sync messages if present
            if "messages" in conversation and isinstance(conversation["messages"], list):
                self._sync_messages(conversation["messages"], conversation_id)
            
            return True
        except Exception as e:
            logger.error(f"Error syncing conversation {conversation_id}: {str(e)}")
            return False
    
    def _sync_messages(self, messages: List[Dict[str, Any]], conversation_id: str) -> int:
        """
        Sync messages for a conversation.
        
        Args:
            messages: List of messages
            conversation_id: Conversation ID
            
        Returns:
            Number of messages synced
        """
        synced_count = 0
        
        for message in messages:
            message_id = message.get("message_id") or message.get("id")
            
            if not message_id:
                logger.warning("Message missing ID, skipping")
                continue
            
            try:
                # Map message to NocoDB format
                nocodb_message = self._map_mongodb_to_nocodb_message(message, conversation_id)
                
                # Check if message exists
                response = self.nocodb_client.fetch_records(
                    table_name=self.nocodb_messages_table,
                    where=f"(id,eq,{message_id})"
                )
                
                exists = response.get("list") and len(response["list"]) > 0
                
                # Create or update message
                # Format the request as { conversation_id, data } where data is the entire message object
                if exists:
                    logger.debug(f"Updating message {message_id} in NocoDB")
                    request_data = {
                        "conversation_id": conversation_id,
                        "data": nocodb_message
                    }
                    self.nocodb_client.update_record(
                        table_name=self.nocodb_messages_table,
                        record_id=message_id,
                        updates=request_data
                    )
                else:
                    logger.debug(f"Creating message {message_id} in NocoDB")
                    request_data = {
                        "conversation_id": conversation_id,
                        "data": nocodb_message
                    }
                    self.nocodb_client.create_record(
                        table_name=self.nocodb_messages_table,
                        record=request_data
                    )
                
                synced_count += 1
            except Exception as e:
                logger.error(f"Error syncing message {message_id}: {str(e)}")
        
        return synced_count
    
    def sync(
        self,
        days_ago: Optional[int] = None,
        start_time: Optional[str] = None,
        limit: Optional[int] = None,
        force_full_sync: bool = False
    ) -> Dict[str, Any]:
        """
        Sync conversations from MongoDB to NocoDB.
        
        Args:
            days_ago: Number of days ago to start syncing from
            start_time: Start time for syncing (ISO format)
            limit: Maximum number of conversations to sync
            force_full_sync: Whether to force a full sync ignoring last sync state
            
        Returns:
            Sync statistics
        """
        # Determine start time
        if force_full_sync:
            sync_start_time = None
            sync_start_id = None
        elif start_time:
            sync_start_time = start_time
            sync_start_id = None
        elif days_ago:
            sync_start_time = (datetime.now() - timedelta(days=days_ago)).isoformat()
            sync_start_id = None
        elif self.last_sync_time:
            sync_start_time = self.last_sync_time
            sync_start_id = self.last_conversation_id
        else:
            # Default to last 7 days if no other criteria
            sync_start_time = (datetime.now() - timedelta(days=7)).isoformat()
            sync_start_id = None
        
        logger.info(f"Starting sync from time: {sync_start_time}, ID: {sync_start_id}")
        
        # Initialize counters
        total_synced = 0
        total_messages = 0
        total_errors = 0
        last_id = sync_start_id
        
        # Process in batches
        while True:
            # Get batch of conversations
            conversations = self._get_conversations_from_mongodb(
                start_time=sync_start_time,
                start_id=last_id,
                limit=self.batch_size
            )
            
            if not conversations:
                logger.info("No more conversations to sync")
                break
            
            logger.info(f"Processing batch of {len(conversations)} conversations")
            
            # Process each conversation
            for conversation in conversations:
                conversation_id = conversation.get("_id") or conversation.get("id")
                
                if not conversation_id:
                    logger.error("Conversation missing ID, skipping")
                    total_errors += 1
                    continue
                
                # Sync conversation
                success = self._sync_conversation(conversation)
                
                if success:
                    total_synced += 1
                    
                    # Count messages
                    if "messages" in conversation and isinstance(conversation["messages"], list):
                        total_messages += len(conversation["messages"])
                    
                    # Update last synced ID
                    last_id = conversation_id
                    
                    # Update last sync time
                    if "created_at" in conversation:
                        self.last_sync_time = conversation["created_at"]
                else:
                    total_errors += 1
            
            # Update state
            self.state.set("last_sync_time", self.last_sync_time)
            self.state.set("last_conversation_id", last_id)
            self.state.save()
            
            # Check if we've reached the limit
            if limit and total_synced >= limit:
                logger.info(f"Reached limit of {limit} conversations")
                break
            
            # If we got fewer conversations than the batch size, we're done
            if len(conversations) < self.batch_size:
                logger.info("Reached end of conversations")
                break
            
            # Small delay to avoid overloading the API
            time.sleep(0.1)
        
        # Final state update
        self.state.set("last_sync_time", self.last_sync_time)
        self.state.set("last_conversation_id", last_id)
        self.state.set("last_sync_completed", datetime.now().isoformat())
        self.state.save()
        
        # Return statistics
        return {
            "conversations_synced": total_synced,
            "messages_synced": total_messages,
            "errors": total_errors,
            "start_time": sync_start_time,
            "end_time": datetime.now().isoformat(),
            "last_conversation_id": last_id
        }


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Sync conversations from MongoDB to NocoDB")
    
    parser.add_argument(
        "--days",
        type=int,
        help="Number of days ago to start syncing from"
    )
    
    parser.add_argument(
        "--start-time",
        type=str,
        help="Start time for syncing (ISO format)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of conversations to sync"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of conversations to process in each batch"
    )
    
    parser.add_argument(
        "--force-full-sync",
        action="store_true",
        help="Force a full sync ignoring last sync state"
    )
    
    parser.add_argument(
        "--state-file",
        type=str,
        default="sync_state.json",
        help="File to store sync state"
    )
    
    args = parser.parse_args()
    
    # Create syncer
    syncer = MongoDBToNocoDBSyncer(
        state_file=args.state_file,
        batch_size=args.batch_size
    )
    
    # Run sync
    start_time = time.time()
    logger.info("Starting sync")
    
    stats = syncer.sync(
        days_ago=args.days,
        start_time=args.start_time,
        limit=args.limit,
        force_full_sync=args.force_full_sync
    )
    
    # Log results
    elapsed = time.time() - start_time
    logger.info(f"Sync completed in {elapsed:.2f} seconds")
    logger.info(f"Synced {stats['conversations_synced']} conversations with {stats['messages_synced']} messages")
    logger.info(f"Encountered {stats['errors']} errors")
    
    if stats["errors"] > 0:
        logger.warning("Sync completed with errors")
        return 1
    
    logger.info("Sync completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())

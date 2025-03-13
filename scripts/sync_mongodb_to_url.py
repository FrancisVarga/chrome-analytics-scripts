#!/usr/bin/env python
"""
Script to sync conversations from MongoDB to a URL with POST requests.

This script retrieves conversations from MongoDB and syncs them to a specified URL
using POST requests. It supports incremental syncing by tracking the last synced conversation.
"""

import argparse
import logging
import os
import signal
import sys
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Add parent directory to path to import from analytics_framework
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics_framework.storage.mongodb.client import MongoDBClient
from analytics_framework.utils.http_client import HTTPClient
from analytics_framework.utils.processing_state import ProcessingState
from analytics_framework.config import (
    MONGODB_URI,
    MONGODB_DATABASE,
    MONGODB_CONVERSATIONS_COLLECTION,
    MAX_RETRIES,
    RETRY_DELAY,
    IO_THREADS,
    BATCH_SIZE
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/sync_mongodb_to_url.log')
    ]
)

logger = logging.getLogger(__name__)

# Global variable to track if the script should exit
should_exit = False

def signal_handler(sig, frame):
    """Handle interrupt signals."""
    global should_exit
    logger.info("Interrupt received, stopping sync gracefully...")
    should_exit = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Termination signal


class MongoDBToURLSyncer:
    """Class to sync conversations from MongoDB to a URL."""
    
    def __init__(
        self,
        url: str,
        mongodb_uri: str = MONGODB_URI,
        mongodb_database: str = MONGODB_DATABASE,
        mongodb_collection: str = MONGODB_CONVERSATIONS_COLLECTION,
        headers: Optional[Dict[str, str]] = None,
        state_file: str = "sync_url_state.json",
        batch_size: int = BATCH_SIZE,
        max_retries: int = MAX_RETRIES,
        retry_delay: int = RETRY_DELAY,
        max_workers: int = IO_THREADS
    ):
        """
        Initialize the syncer.
        
        Args:
            url: URL to send data to
            mongodb_uri: MongoDB connection URI
            mongodb_database: MongoDB database name
            mongodb_collection: MongoDB collection name for conversations
            headers: HTTP headers for the requests
            state_file: File to store sync state
            batch_size: Number of conversations to process in each batch
            max_retries: Maximum number of retries for failed requests
            retry_delay: Delay between retries in seconds
            max_workers: Maximum number of worker threads for parallel requests
        """
        self.url = url
        self.mongodb_collection = mongodb_collection
        self.batch_size = batch_size
        self.headers = headers or {}
        
        # Initialize MongoDB client
        self.mongodb_client = MongoDBClient(
            uri=mongodb_uri,
            database=mongodb_database
        )
        
        # Initialize HTTP client with increased parallelism
        self.http_client = HTTPClient(
            max_retries=max_retries,
            retry_delay=retry_delay,
            max_workers=max_workers * 2  # Double the default worker threads for more parallelism
        )
        
        # Initialize processing state
        self.state = ProcessingState(state_file)
        
        # Load last sync state
        self.last_sync_time = self.state.state.get("last_sync_time")
        self.last_conversation_id = self.state.state.get("last_conversation_id")
        
        logger.info(f"Initialized syncer with last sync time: {self.last_sync_time}")
    
    def _prepare_conversation_for_sync(self, conversation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a conversation for syncing.
        
        Args:
            conversation: MongoDB conversation document
            
        Returns:
            Prepared conversation data
        """
        # Create a copy to avoid modifying the original
        prepared_data = conversation.copy()
        
        # Convert non-JSON serializable values
        self._make_json_serializable(prepared_data)
        
        return prepared_data
    
    def _make_json_serializable(self, data: Any) -> None:
        """
        Recursively convert non-JSON serializable values in a dictionary or list.
        
        Args:
            data: Dictionary or list to convert
        """
        if isinstance(data, dict):
            for key, value in list(data.items()):
                if isinstance(value, (dict, list)):
                    self._make_json_serializable(value)
                elif isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
                    # Handle NaN and infinity values
                    data[key] = str(value)
                elif not isinstance(value, (str, int, float, bool, type(None))):
                    # Convert any other non-serializable types to string
                    data[key] = str(value)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, (dict, list)):
                    self._make_json_serializable(item)
                elif isinstance(item, float) and (item != item or item == float('inf') or item == float('-inf')):
                    # Handle NaN and infinity values
                    data[i] = str(item)
                elif not isinstance(item, (str, int, float, bool, type(None))):
                    # Convert any other non-serializable types to string
                    data[i] = str(item)
    
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
    
    def _sync_conversation(self, conversation: Dict[str, Any]) -> bool:
        """
        Sync a single conversation to the URL.
        
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
            # Prepare conversation data
            prepared_data = self._prepare_conversation_for_sync(conversation)
            self.headers["Content-Type"] = "application/json"
            self.headers["accept"] = "application/json"
            
            # Send data to URL
            response = self.http_client.post(
                endpoint=self.url,
                json={
                    "conversation_id": conversation_id,
                    "data":prepared_data
                },
                headers=self.headers
            )
            
            # Check response
            if response.status_code >= 200 and response.status_code < 300:
                logger.debug(f"Successfully synced conversation {conversation_id}")
                return True
            else:
                logger.error(f"Error syncing conversation {conversation_id}: HTTP {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing conversation {conversation_id}: {str(e)}")
            return False
    
    def _sync_batch(self, conversations: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Sync a batch of conversations.
        
        Args:
            conversations: List of conversations to sync
            
        Returns:
            Dictionary with success and error counts
        """
        if not conversations:
            return {"success": 0, "error": 0}
        
        # Prepare data for parallel requests
        requests_data = []
        for conversation in conversations:
            prepared_data = self._prepare_conversation_for_sync(conversation)
            
            requests_data.append({
                "method": "POST",
                "endpoint": self.url,
                "json": {
                    "conversation_id": conversation.get("_id") or conversation.get("id"),
                    "data":prepared_data
                },
                "headers": self.headers
            })
        
        # Send requests in parallel
        responses = self.http_client.parallel_requests(requests_data)
        
        # Process responses
        success_count = 0
        error_count = 0
        
        for i, response in enumerate(responses):
            conversation_id = conversations[i].get("_id") or conversations[i].get("id")
            
            if isinstance(response, Exception):
                logger.error(f"Error syncing conversation {conversation_id}: {str(response)}")
                error_count += 1
            elif response.status_code >= 200 and response.status_code < 300:
                logger.debug(f"Successfully synced conversation {conversation_id}")
                success_count += 1
            else:
                logger.error(f"Error syncing conversation {conversation_id}: HTTP {response.status_code} - {response.text}")
                error_count += 1
        
        return {"success": success_count, "error": error_count}
    
    def sync(
        self,
        days_ago: Optional[int] = None,
        start_time: Optional[str] = None,
        limit: Optional[int] = None,
        force_full_sync: bool = False,
        parallel: bool = True
    ) -> Dict[str, Any]:
        """
        Sync conversations from MongoDB to the URL.
        
        Args:
            days_ago: Number of days ago to start syncing from
            start_time: Start time for syncing (ISO format)
            limit: Maximum number of conversations to sync
            force_full_sync: Whether to force a full sync ignoring last sync state
            parallel: Whether to use parallel requests for batches
            
        Returns:
            Sync statistics
        """
        global should_exit
        
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
        total_errors = 0
        last_id = sync_start_id
        
        # Process in batches
        while not should_exit:
            # Check if we should exit
            if should_exit:
                logger.info("Stopping sync due to interrupt signal")
                break
                
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
            
            if parallel and len(conversations) > 1:
                # Sync batch in parallel
                batch_result = self._sync_batch(conversations)
                total_synced += batch_result["success"]
                total_errors += batch_result["error"]
                
                # Update last synced ID
                if conversations:
                    last_conversation = conversations[-1]
                    last_id = last_conversation.get("_id") or last_conversation.get("id")
                    
                    # Update last sync time
                    if "created_at" in last_conversation:
                        self.last_sync_time = last_conversation["created_at"]
            else:
                # Process each conversation sequentially
                for conversation in conversations:
                    # Check if we should exit
                    if should_exit:
                        logger.info("Stopping sync due to interrupt signal")
                        break
                        
                    conversation_id = conversation.get("_id") or conversation.get("id")
                    
                    if not conversation_id:
                        logger.error("Conversation missing ID, skipping")
                        total_errors += 1
                        continue
                    
                    # Sync conversation
                    success = self._sync_conversation(conversation)
                    
                    if success:
                        total_synced += 1
                        
                        # Update last synced ID
                        last_id = conversation_id
                        
                        # Update last sync time
                        if "created_at" in conversation:
                            self.last_sync_time = conversation["created_at"]
                    else:
                        total_errors += 1
            
            # Update state
            self.state.state["last_sync_time"] = self.last_sync_time
            self.state.state["last_conversation_id"] = last_id
            self.state.save()
            
            # Check if we should exit after processing this batch
            if should_exit:
                logger.info("Stopping sync due to interrupt signal")
                break
            
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
        self.state.state["last_sync_time"] = self.last_sync_time
        self.state.state["last_conversation_id"] = last_id
        self.state.state["last_sync_completed"] = datetime.now().isoformat()
        self.state.save()
        
        # Return statistics
        return {
            "conversations_synced": total_synced,
            "errors": total_errors,
            "start_time": sync_start_time,
            "end_time": datetime.now().isoformat(),
            "last_conversation_id": last_id
        }
    
    def close(self):
        """Close the HTTP client."""
        self.http_client.close()


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Sync conversations from MongoDB to a URL")
    
    parser.add_argument(
        "url",
        type=str,
        help="URL to send data to"
    )
    
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
        default=BATCH_SIZE,
        help="Number of conversations to process in each batch"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=IO_THREADS * 2,  # Default to double the IO_THREADS
        help="Number of worker threads for parallel requests"
    )
    
    parser.add_argument(
        "--force-full-sync",
        action="store_true",
        help="Force a full sync ignoring last sync state"
    )
    
    parser.add_argument(
        "--state-file",
        type=str,
        default="sync_url_state.json",
        help="File to store sync state"
    )
    
    parser.add_argument(
        "--header",
        action="append",
        dest="headers",
        help="HTTP header in the format 'key:value' (can be used multiple times)"
    )
    
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Use sequential requests instead of parallel"
    )
    
    args = parser.parse_args()
    
    # Parse headers
    headers = {}
    if args.headers:
        for header in args.headers:
            try:
                key, value = header.split(":", 1)
                headers[key.strip()] = value.strip()
            except ValueError:
                logger.warning(f"Invalid header format: {header}, expected 'key:value'")
    
    # Create syncer
    syncer = MongoDBToURLSyncer(
        url=args.url,
        headers=headers,
        state_file=args.state_file,
        batch_size=args.batch_size,
        max_workers=args.workers
    )
    
    try:
        # Run sync
        start_time = time.time()
        logger.info("Starting sync")
        
        stats = syncer.sync(
            days_ago=args.days,
            start_time=args.start_time,
            limit=args.limit,
            force_full_sync=args.force_full_sync,
            parallel=not args.sequential
        )
        
        # Log results
        elapsed = time.time() - start_time
        logger.info(f"Sync completed in {elapsed:.2f} seconds")
        logger.info(f"Synced {stats['conversations_synced']} conversations")
        logger.info(f"Encountered {stats['errors']} errors")
        
        if stats["errors"] > 0:
            logger.warning("Sync completed with errors")
            return 1
        
        logger.info("Sync completed successfully")
        return 0
    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
        return 1
    finally:
        # Close the syncer
        syncer.close()


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python
"""
Script to fetch yesterday's conversations and send them to a Dify workflow.

This script retrieves all conversations created yesterday from MongoDB
and sends them to a specified Dify workflow for analysis.

Usage:
    python scripts/dify/send_yesterday_conversations.py [--workflow-id WORKFLOW_ID]
                                                      [--batch-size BATCH_SIZE]
                                                      [--limit LIMIT]
                                                      [--app-id APP_ID]
                                                      [--streaming]
                                                      [--log-level {DEBUG,INFO,WARNING,ERROR}]
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

# Add parent directory to Python path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../..')
))

from analytics_framework.storage.mongodb.client import MongoDBClient
from analytics_framework.api.dify_client import DifyClient
from analytics_framework.config import (
    setup_logging,
    DIFY_API_KEY,
    DIFY_BASE_URL,
    MONGODB_URI,
    MONGODB_DATABASE
)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch yesterday's conversations and send them to a Dify workflow"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of conversations to process in each batch (default: 10)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of conversations to process (default: no limit)"
    )
    
    parser.add_argument(
        "--app-id",
        help="Filter conversations by app ID"
    )
    
    parser.add_argument(
        "--streaming",
        action="store_true",
        default=False,
        help="Use streaming response mode instead of blocking"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set the logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--workflow-id",
        default="scripted_workflow",
        help="ID of the Dify workflow to use (default: scripted_workflow)"
    )
    
    return parser.parse_args()


def get_yesterday_range() -> tuple:
    """
    Get the ISO formatted date range for yesterday.
    
    Returns:
        tuple: (start_date, end_date) for yesterday
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    yesterday_end = today - timedelta(microseconds=1)
    
    return yesterday.isoformat(), yesterday_end.isoformat()


def prepare_conversation_for_workflow(conversation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare a conversation for submission to the Dify workflow.
    
    Args:
        conversation: The MongoDB conversation document
        
    Returns:
        Dict with formatted conversation data suitable for Dify
    """
    # Extract key fields that would be relevant for analysis
    messages = []
    for msg in conversation.get('messages', []):
        # Handle different message structures
        content = ''
        
        if isinstance(msg.get('message'), dict):
            # If message is a dictionary, get content from it
            content = msg.get('message', {}).get('content', '')
        elif isinstance(msg.get('message'), list):
            # If message is a list, try to extract content from it
            try:
                # Try to join list items if they're strings
                content = ' '.join([str(item) for item in msg.get('message', [])])
            except:
                # Fallback to empty string if joining fails
                content = ''
        
        # Fallback to direct content if available
        if not content and msg.get('content'):
            content = msg.get('content', '')
            
        messages.append({
            'role': msg.get('role', 'unknown'),
            'content': content,
            'created_at': msg.get('created_at', '')
        })
    
    # Return the processed conversation
    return {
        'conversation_id': conversation.get('_id', ''),
        'app_id': conversation.get('app_id', ''),
        'model_id': conversation.get('model_id', ''),
        'model_provider': conversation.get('model_provider', ''),
        'created_at': conversation.get('created_at', ''),
        'message_count': conversation.get('message_count', 0),
        'total_tokens': conversation.get('total_tokens', 0),
        'total_price': conversation.get('total_price', 0),
        'messages': messages,
        'inputs': conversation.get('inputs', {})
    }


def process_conversation(
    conversation: Dict[str, Any],
    dify_client: DifyClient,
    workflow_id: str,
    streaming: bool = False
) -> Dict[str, Any]:
    """
    Process a single conversation through the Dify workflow.
    
    Args:
        conversation: MongoDB conversation document
        dify_client: Initialized DifyClient
        workflow_id: ID of the Dify workflow to use
        streaming: Whether to use streaming mode
        
    Returns:
        Dict with processing results
    """
    # Prepare conversation data for the workflow
    workflow_input = {
        'content': prepare_conversation_for_workflow(conversation)
    }
    
    # Set response mode based on streaming flag
    response_mode = "streaming" if streaming else "blocking"
    
    try:
        # Call the Dify workflow
        logging.info(f"Sending conversation {conversation.get('_id', '')} to Dify workflow")
        
        if streaming:
            logging.info(f"Executing workflow {workflow_id} in streaming mode")
            logging.debug(f"Workflow input: {workflow_input}")
            # For streaming mode, collect all chunks
            chunks = []
            for chunk in dify_client.execute_workflow(
                inputs=workflow_input,
                response_mode=response_mode,
                user_id=f"system_analytics_{workflow_id}"
            ):
                if chunk.get("event") == "message":
                    if "answer" in chunk:
                        chunks.append(chunk["answer"])
                elif chunk.get("event") == "workflow_finished":
                    if "outputs" in chunk:
                        return {
                            "success": True,
                            "conversation_id": conversation.get('_id', ''),
                            "result": chunk.get("outputs", {}),
                            "streaming_content": "".join(chunks),
                            "processed_at": datetime.now().isoformat(),
                            "workflow_id": workflow_id
                        }
            
            return {
                "success": False,
                "conversation_id": conversation.get('_id', ''),
                "error": "Workflow finished without outputs",
                "streaming_content": "".join(chunks),
                "processed_at": datetime.now().isoformat(),
                "workflow_id": workflow_id
            }
        else:
            import json
            # For blocking mode
            logging.info(f"Executing workflow {workflow_id} in blocking mode")
            logging.info(f"Executing workflow {workflow_id} for conversation {conversation.get('_id', '')}")
            logging.debug(f"Triggering Dify workflow with inputs: {json.dumps(workflow_input, indent=2)}")
            workflow_input["content"] = json.dumps(workflow_input["content"], indent=2)
            result = dify_client.execute_workflow(
                inputs=workflow_input,
                response_mode="blocking",
                user_id="system_analytics"
            )
            
            if "data" in result and "outputs" in result["data"]:
                return {
                    "success": True,
                    "conversation_id": conversation.get('_id', ''),
                    "result": result["data"]["outputs"],
                    "workflow_run_id": result.get("workflow_run_id", ""),
                    "processed_at": datetime.now().isoformat(),
                    "workflow_id": workflow_id
                }
            else:
                return {
                    "success": False,
                    "conversation_id": conversation.get('_id', ''),
                    "error": "No outputs in workflow result",
                    "workflow_run_id": result.get("workflow_run_id", ""),
                    "processed_at": datetime.now().isoformat(),
                    "workflow_id": workflow_id
                }
                
    except Exception as e:
        logging.error(f"Error processing conversation {conversation.get('_id', '')}: {str(e)}")
        return {
            "success": False,
            "conversation_id": conversation.get('_id', ''),
            "error": str(e),
            "processed_at": datetime.now().isoformat(),
            "workflow_id": workflow_id
        }


def save_dify_result_to_mongodb(
    mongodb_client: MongoDBClient,
    result: Dict[str, Any],
    collection_name: str = "dify_workflow_results"
) -> None:
    """
    Save Dify workflow result to MongoDB.
    
    Args:
        mongodb_client: Initialized MongoDB client
        result: Dify workflow result
        collection_name: Name of the collection to save to
    """
    try:
        document = {
            "_id": f"{result['conversation_id']}_{result['workflow_id']}",
            **result
        }
        
        # Store the result using the base client's replace_one method
        mongodb_client.base_client.replace_one(
            collection_name,
            {"_id": document["_id"]},
            document,
            upsert=True
        )
        
        logging.info(f"Saved Dify workflow result for conversation {result['conversation_id']} in MongoDB")
    except Exception as e:
        logging.error(f"Error saving Dify workflow result: {str(e)}")


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Set up logging
    # log_level = getattr(logging, args.log_level)
    setup_logging()
    
    # Get yesterday's date range
    start_date, end_date = get_yesterday_range()
    logging.info(f"Fetching conversations from {start_date} to {end_date}")
    
    # Initialize clients
    mongodb_client = MongoDBClient(
        uri=MONGODB_URI,
        database=MONGODB_DATABASE
    )
    
    dify_client = DifyClient(
        api_key=DIFY_API_KEY,
        base_url=DIFY_BASE_URL
    )
    
    # Build the query
    query = {
        "created_at": {
            "$gte": start_date,
            "$lte": end_date
        },
        "is_deleted": False
    }
    
    if args.app_id:
        query["app_id"] = args.app_id
        
    # Count total conversations using the conversation client
    total_conversations = mongodb_client.conversation.get_conversation_count_by_date_range(
        start_date,
        end_date
    )
    
    # Apply additional filter for app_id if specified
    if args.app_id:
        # For app_id filtering, we need to use the base count_documents since
        # there's no specific method for counting with both date range and app_id
        total_conversations = mongodb_client.base_client.count_documents(
            "conversations",
            query
        )
        
    if total_conversations == 0:
        logging.info("No conversations found for yesterday")
        return
        
    logging.info(f"Found {total_conversations} conversations from yesterday")
    
    # Apply limit if specified
    limit = args.limit if args.limit is not None else total_conversations
    logging.info(f"Processing up to {limit} conversations")
    
    # Process conversations in batches
    batch_size = args.batch_size
    processed_count = 0
    success_count = 0
    error_count = 0
    
    for skip in range(0, limit, batch_size):
        # Adjust batch size for the last batch if needed
        current_batch_size = min(batch_size, limit - skip)
        
        # Fetch a batch of conversations - using base_client.find since we need
        # to apply custom query criteria
        conversations = mongodb_client.base_client.find(
            "conversations",
            query,
            sort=[("created_at", 1)],
            limit=current_batch_size,
            skip=skip
        )
        
        logging.info(f"Processing batch of {len(conversations)} conversations")
        
        # Process each conversation in the batch
        for conversation in conversations:
            result = process_conversation(
                conversation,
                dify_client,
                args.workflow_id,
                args.streaming
            )
            
            processed_count += 1
            
            if result["success"]:
                success_count += 1
                logging.info(f"Successfully processed conversation {result['conversation_id']}")
            else:
                error_count += 1
                logging.error(f"Failed to process conversation {result['conversation_id']}: {result.get('error', 'Unknown error')}")
                
            # Store the result in MongoDB
            save_dify_result_to_mongodb(mongodb_client, result)
            
            # Also update the conversation document with a reference to the analysis
            mongodb_client.base_client.update_one(
                "conversations",
                {"_id": result["conversation_id"]},
                {
                    "$set": {
                        "dify_analysis_ref": f"{result['conversation_id']}_{result['workflow_id']}_{result['processed_at']}",
                        "dify_analysis_success": result["success"],
                        "dify_analysis_workflow_id": result["workflow_id"],
                        "dify_analysis_processed_at": result["processed_at"]
                    }
                }
            )
    
    # Log summary
    logging.info(f"Processing complete. Processed: {processed_count}, Success: {success_count}, Errors: {error_count}")


if __name__ == "__main__":
    main()
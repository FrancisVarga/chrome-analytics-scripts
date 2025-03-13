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
    yesterday = today - timedelta(days=14)
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
    import json
    
    # Helper function to ensure a value is JSON serializable
    def ensure_serializable(value):
        try:
            json.dumps(value)
            return value
        except (TypeError, OverflowError):
            if isinstance(value, dict):
                return {k: ensure_serializable(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [ensure_serializable(item) for item in value]
            else:
                return str(value)
    
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
    
    # Prepare the conversation data
    conversation_data = {
        'conversation_id': conversation.get('_id', ''),
        'app_id': conversation.get('app_id', ''),
        'model_id': conversation.get('model_id', ''),
        'model_provider': conversation.get('model_provider', ''),
        'created_at': conversation.get('created_at', ''),
        'message_count': conversation.get('message_count', 0),
        'total_tokens': conversation.get('total_tokens', 0),
        'total_price': conversation.get('total_price', 0),
        'messages': messages,
        'inputs': ensure_serializable(conversation.get('inputs', {}))
    }
    
    # Ensure the entire object is JSON serializable
    try:
        json.dumps(conversation_data)
        return conversation_data
    except (TypeError, OverflowError):
        logging.warning("Conversation data contains non-serializable values, sanitizing...")
        return ensure_serializable(conversation_data)


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
    import json
    
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
                        # Ensure outputs are JSON-serializable
                        try:
                            outputs = chunk.get("outputs", {})
                            # Test JSON serialization
                            json.dumps(outputs)
                            sanitized_outputs = outputs
                        except (TypeError, OverflowError):
                            # If not serializable, convert to string
                            logging.warning("Workflow outputs contain non-serializable data, converting to string")
                            sanitized_outputs = json.loads(json.dumps(str(outputs)))
                            
                        return {
                            "success": True,
                            "conversation_id": conversation.get('_id', ''),
                            "result": sanitized_outputs,
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
            # For blocking mode
            logging.info(f"Executing workflow {workflow_id} in blocking mode")
            logging.info(f"Executing workflow {workflow_id} for conversation {conversation.get('_id', '')}")
            logging.debug(f"Triggering Dify workflow with inputs: {json.dumps(workflow_input, indent=2)}")
            workflow_input["conversation"] = json.dumps(workflow_input["content"], indent=2)
            result = dify_client.execute_workflow(
                inputs=workflow_input,
                response_mode="blocking",
                user_id="system_analytics"
            )
            
            if "data" in result and "outputs" in result["data"]:
                # Ensure outputs are JSON-serializable
                try:
                    outputs = result["data"]["outputs"]
                    # Test JSON serialization
                    json.dumps(outputs)
                    sanitized_outputs = outputs
                except (TypeError, OverflowError):
                    # If not serializable, convert to string
                    logging.warning("Workflow outputs contain non-serializable data, converting to string")
                    try:
                        sanitized_outputs = json.loads(json.dumps(str(outputs)))
                    except:
                        sanitized_outputs = str(outputs)
                        
                return {
                    "success": True,
                    "conversation_id": conversation.get('_id', ''),
                    "result": sanitized_outputs,
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
    collection_name: str = "success_conversations"
) -> None:
    """
    Save Dify workflow result to MongoDB.
    
    Args:
        mongodb_client: Initialized MongoDB client
        result: Dify workflow result
        collection_name: Name of the collection to save to
    """
    try:
        import json
        from bson import json_util
        
        # Create a sanitized copy of the result
        sanitized_result = {}
        
        # Process each key in the result to ensure JSON compatibility
        for key, value in result.items():
            try:
                # Test if the value is JSON serializable
                json.dumps(value)
                sanitized_result[key] = value
            except (TypeError, OverflowError):
                # If not serializable, convert to string representation
                try:
                    # Try using BSON's json_util for MongoDB-compatible serialization
                    sanitized_result[key] = json_util.loads(json_util.dumps(value))
                except Exception:
                    # Fallback to string representation if json_util fails
                    sanitized_result[key] = str(value)
                    logging.warning(f"Converted non-serializable value for key '{key}' to string")
        
        # Create document with sanitized result
        document = {
            "_id": f"{sanitized_result['conversation_id']}_{sanitized_result['workflow_id']}",
            **sanitized_result
        }
        
        # Validate the document is JSON serializable
        try:
            json.dumps(document)
        except (TypeError, OverflowError) as e:
            logging.error(f"Document still not JSON serializable after sanitization: {str(e)}")
            # Create a simplified document with just the essential fields
            document = {
                "_id": f"{result['conversation_id']}_{result['workflow_id']}",
                "conversation_id": result.get('conversation_id', ''),
                "workflow_id": result.get('workflow_id', ''),
                "success": result.get('success', False),
                "processed_at": result.get('processed_at', datetime.now().isoformat()),
                "error": "Document contained non-serializable data"
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
            try:
                # Create a JSON-serializable update document
                update_doc = {
                    "dify_analysis_ref": f"{result['conversation_id']}_{result['workflow_id']}_{result['processed_at']}",
                    "dify_analysis_success": result["success"],
                    "dify_analysis_workflow_id": result["workflow_id"],
                    "dify_analysis_processed_at": result["processed_at"]
                }
                
                # Ensure the update document is JSON-serializable
                import json
                json.dumps(update_doc)  # This will raise an exception if not serializable
                
                mongodb_client.base_client.update_one(
                    "conversations",
                    {"_id": result["conversation_id"]},
                    {"$set": update_doc}
                )
                logging.info(f"Updated conversation {result['conversation_id']} with analysis reference")
            except Exception as e:
                logging.error(f"Error updating conversation with analysis reference: {str(e)}")
    
    # Log summary
    logging.info(f"Processing complete. Processed: {processed_count}, Success: {success_count}, Errors: {error_count}")


if __name__ == "__main__":
    main()
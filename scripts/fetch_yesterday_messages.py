#!/usr/bin/env python
"""
Script to fetch conversations from MongoDB for a specific day and extract only the messages.

This script connects to MongoDB, retrieves all conversations created on a specific day,
and extracts the messages from those conversations. By default, it fetches yesterday's
conversations, but you can specify a different number of days ago. The results can be
printed to the console or saved to a file.

Usage:
    # On Windows:
    python scripts/fetch_yesterday_messages.py [--output OUTPUT_FILE] [--days-ago DAYS] [--mongodb-uri URI] [--mongodb-database DB]
    
    # On Unix-like systems (if script has execute permissions):
    ./scripts/fetch_yesterday_messages.py [--output OUTPUT_FILE] [--days-ago DAYS] [--mongodb-uri URI] [--mongodb-database DB]
    
    # To save output to a file:
    python scripts/fetch_yesterday_messages.py --output yesterday_messages.json
    
    # To fetch conversations from 3 days ago:
    python scripts/fetch_yesterday_messages.py --days-ago 3
    
    # To use custom MongoDB connection:
    python scripts/fetch_yesterday_messages.py --mongodb-uri "mongodb://user:pass@host:port" --mongodb-database "my_db"
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add the parent directory to the path so we can import the analytics_framework
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics_framework.storage.mongodb.client import MongoDBClient
from analytics_framework.config import MONGODB_URI, MONGODB_DATABASE

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_date_range_for_days_ago(days_ago: int = 1) -> tuple:
    """
    Get the date range for a specific number of days ago.
    
    Args:
        days_ago: Number of days ago (default: 1 for yesterday)
        
    Returns:
        Tuple of (start_date, end_date) in ISO format
    """
    today = datetime.now()
    target_date = today - timedelta(days=days_ago)
    
    # Set time to start of day
    date_start = datetime(
        target_date.year, target_date.month, target_date.day, 0, 0, 0
    )
    
    # Set time to end of day
    date_end = datetime(
        target_date.year, target_date.month, target_date.day, 23, 59, 59, 999999
    )
    
    return date_start.isoformat(), date_end.isoformat()


def fetch_conversations_for_date(mongodb_client: MongoDBClient, days_ago: int = 1) -> List[Dict[str, Any]]:
    """
    Fetch all conversations created on a specific date.
    
    Args:
        mongodb_client: MongoDB client
        days_ago: Number of days ago (default: 1 for yesterday)
        
    Returns:
        List of conversations
    """
    start_date, end_date = get_date_range_for_days_ago(days_ago)
    
    date_description = "yesterday" if days_ago == 1 else f"{days_ago} days ago"
    logger.info(f"Fetching conversations from {date_description} ({start_date} to {end_date})")
    
    conversations = mongodb_client.conversation.get_conversations_by_date_range(
        start_date=start_date,
        end_date=end_date,
        limit=0  # No limit, get all conversations
    )
    
    logger.info(f"Found {len(conversations)} conversations from {date_description}")
    return conversations


def format_conversation(messages: List[Dict[str, Any]], conversation: Dict[str, Any]) -> str:
    """
    Format a conversation as a human-readable string with the format:
    User AI {timestamp}: {answer} (for AI responses)
    User {username} {timestamp}: {query} (for user messages)
    
    Args:
        messages: List of messages in the conversation
        conversation: The conversation object containing metadata
        
    Returns:
        Formatted string representation of the conversation
    """
    formatted_conversation = ""
    
    # Get the username from the conversation inputs
    inputs = conversation.get("inputs", {})
    username = inputs.get("username", "User") if inputs else "User"
    
    # In the sample data, each message contains both the query and answer
    for message in messages:
        # Get timestamp
        timestamp = message.get("created_at", "")
        
        # Get query (user message)
        query = message.get("query", "")
        
        # Get answer (AI response)
        answer = message.get("answer", "")
        
        # Format the user message
        if query:
            formatted_message = f"User {username} {timestamp}: {query}\n"
            formatted_conversation += formatted_message
        
        # Format the AI response
        if answer:
            formatted_message = f"User AI {timestamp}: {answer}\n"
            formatted_conversation += formatted_message
    
    return formatted_conversation


def extract_messages_from_conversations(conversations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract messages from conversations, grouping them by conversation.
    
    Args:
        conversations: List of conversations
        
    Returns:
        List of conversation objects, each containing its messages
    """
    # Save a sample of raw messages for debugging
    if conversations:
        sample_size = min(5, len(conversations))
        sample_conversations = conversations[:sample_size]
        with open('tmp/sample_raw_messages.json', 'w', encoding='utf-8') as f:
            json.dump(sample_conversations, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {sample_size} sample conversations to tmp/sample_raw_messages.json")
    
    conversation_messages = []
    total_messages = 0
    
    for conversation in conversations:
        conversation_id = conversation.get("_id")
        conversation_name = conversation.get("name", "Unnamed Conversation")
        app_id = conversation.get("app_id")
        model_id = conversation.get("model_id")
        from_end_user_id = conversation.get("from_end_user_id")
        created_at = conversation.get("created_at")
        
        # Extract messages from the conversation
        messages = conversation.get("messages", [])
        total_messages += len(messages)
        
        # Get the inputs object from the conversation
        inputs = conversation.get("inputs", {})
        
        # Create a conversation object with all its messages
        conversation_with_messages = {
            "conversation_id": conversation_id,
            "conversation_name": conversation_name,
            "app_id": app_id,
            "model_id": model_id,
            "from_end_user_id": from_end_user_id,
            "created_at": created_at,
            "inputs": inputs,  # Include the inputs object
            "message_count": len(messages),
            "messages": []
        }
        
        # Process all messages for the conversation
        processed_messages = []
        for message in messages:
            # Check for different possible content fields
            content = message.get("content")
            if content is None:
                # Try alternative fields that might contain the message content
                content = message.get("message") or message.get("answer") or message.get("query") or ""
            
            message_data = {
                "message_id": message.get("message_id"),
                "sequence_number": message.get("sequence_number"),
                "role": message.get("role"),
                "content": content,
                "query": message.get("query"),  # Include the query field
                "message": message.get("message"),  # Include the message field
                "answer": message.get("answer"),  # Include the answer field
                "tokens": message.get("tokens"),
                "message_tokens": message.get("message_tokens"),  # Include message_tokens
                "answer_tokens": message.get("answer_tokens"),  # Include answer_tokens
                "price": message.get("price"),
                "total_price": message.get("total_price"),  # Include total_price
                "created_at": message.get("created_at"),
                "parent_message_id": message.get("parent_message_id"),
                "message_metadata": message.get("message_metadata")  # Include message_metadata
            }
            
            # Log if content is still empty
            if not content and logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Message {message.get('message_id')} has no content. Original message: {message}")
            
            processed_messages.append(message_data)
        
        # Add all processed messages to the conversation object
        conversation_with_messages["messages"] = processed_messages
        
        # Format the conversation and add it to the object
        formatted_conversation = format_conversation(processed_messages, conversation)
        conversation_with_messages["formatted_conversation"] = formatted_conversation
        
        conversation_messages.append(conversation_with_messages)
    
    logger.info(f"Extracted {total_messages} messages from {len(conversations)} conversations")
    return conversation_messages


def save_messages_to_file(messages: List[Dict[str, Any]], output_file: str):
    """
    Save messages to a JSON file.
    
    Args:
        messages: List of messages
        output_file: Output file path
    """
    # Create the output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            logger.info(f"Created directory: {output_dir}")
        except OSError as e:
            logger.error(f"Error creating directory {output_dir}: {str(e)}")
            raise
    
    # Save the messages to the file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(messages, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Saved {len(messages)} messages to {output_file}")


def main():
    """Main function."""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Fetch conversation messages from MongoDB for a specific day')
    parser.add_argument('--output', '-o', type=str, help='Output file path (default: print to console)')
    parser.add_argument('--days-ago', '-d', type=int, default=1, help='Number of days ago (default: 1 for yesterday)')
    parser.add_argument('--mongodb-uri', type=str, default=MONGODB_URI, help='MongoDB URI')
    parser.add_argument('--mongodb-database', type=str, default=MONGODB_DATABASE, help='MongoDB database name')
    args = parser.parse_args()
    
    try:
        # Initialize MongoDB client
        mongodb_client = MongoDBClient(
            uri=args.mongodb_uri,
            database=args.mongodb_database
        )
        
        # Check MongoDB connection
        if not mongodb_client.ping():
            logger.error("Failed to connect to MongoDB")
            return 1
        
        # Fetch conversations for the specified date
        conversations = fetch_conversations_for_date(mongodb_client, args.days_ago)
        
        # Extract messages from conversations
        messages = extract_messages_from_conversations(conversations)
        
        # Output messages
        if args.output:
            save_messages_to_file(messages, args.output)
        else:
            # Print messages to console
            print(json.dumps(messages, indent=2, ensure_ascii=False))
        
        logger.info("Script completed successfully")
        return 0
    
    except Exception as e:
        logger.exception(f"Error: {str(e)}")
        return 1
    
    finally:
        # Close MongoDB connection
        if 'mongodb_client' in locals():
            mongodb_client.close()


if __name__ == "__main__":
    sys.exit(main())

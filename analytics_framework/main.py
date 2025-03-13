"""Main entry point for the analytics framework."""

import argparse
import logging
from typing import Optional, Tuple

from .config import (
    BATCH_SIZE,
    NOCODB_CONVERSATION_TABLE,
    NOCODB_MESSAGES_TABLE,
    MONGODB_CONVERSATIONS_COLLECTION,
    MONGODB_ANALYTICS_REPORTS_COLLECTION,
    MONGODB_USER_ANALYTICS_COLLECTION,
    S3_ENABLED,
    S3_BUCKET,
    S3_PREFIX,
    PARQUET_STORAGE_ENABLED,
    setup_logging,
    validate_config
)
from .api.nocodb_client import NocoDBClient
from .storage.mongodb_client import MongoDBClient
from .storage.parquet_storage import ParquetStorage
from .processors.data_processor import DataProcessor
from .utils.processing_state import create_processing_state


def create_mongodb_indexes(mongo_client: MongoDBClient) -> None:
    """
    Create MongoDB indexes.
    
    Args:
        mongo_client: MongoDB client
    """
    # Indexes are created automatically when the MongoDBClient is initialized
    pass


def process_conversation_batch(
    nocodb_client: NocoDBClient,
    mongo_client: Optional[MongoDBClient],
    parquet_storage: Optional[ParquetStorage],
    data_processor: DataProcessor,
    processing_state,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    app_id: Optional[str] = None,
    last_id: Optional[str] = None,
    batch_size: int = BATCH_SIZE
) -> Tuple[int, Optional[str]]:
    """
    Process a batch of conversations.
    
    Args:
        nocodb_client: NocoDB client
        mongo_client: MongoDB client (can be None to skip MongoDB storage)
        data_processor: Data processor
        processing_state: Processing state tracker
        start_date: Start date for filtering conversations
        end_date: End date for filtering conversations
        app_id: App ID for filtering conversations
        last_id: Last processed conversation ID
        batch_size: Batch size
        
    Returns:
        Tuple of (processed_count, last_processed_id)
    """
    # Build where clause
    where_clauses = []
    
    if last_id:
        where_clauses.append(f"(id,gt,{last_id})")
        
    if start_date:
        where_clauses.append(f"(created_at,gte,{start_date})")
        
    if end_date:
        where_clauses.append(f"(created_at,lt,{end_date})")
        
    if app_id:
        where_clauses.append(f"(app_id,eq,{app_id})")
    
    where_clause = "~and".join(where_clauses) if where_clauses else None
    
    # Fetch conversations
    conversations = nocodb_client.fetch_records(
        NOCODB_CONVERSATION_TABLE,
        where=where_clause,
        sort="id",
        limit=batch_size
    )
    
    conversation_list = conversations.get("list", [])
    if not conversation_list:
        return 0, last_id
    
    logging.info(f"Processing batch of {len(conversation_list)} conversations")
    
    # Process each conversation
    processed_count = 0
    last_processed_id = last_id
    processed_conversations = []
    
    # Fetch messages for all conversations in batch
    conversation_ids = [conv["id"] for conv in conversation_list]
    messages_by_conversation = {}
    
    for conversation_id in conversation_ids:
        messages = nocodb_client.fetch_all_records(
            NOCODB_MESSAGES_TABLE,
            where=f"(conversation_id,eq,{conversation_id})",
            sort="created_at"
        )
        messages_by_conversation[conversation_id] = messages
    
    # Process conversations in parallel
    processed_docs = data_processor.process_conversations_batch(
        conversation_list,
        messages_by_conversation
    )
    
    # Extract categories for all conversations
    categories_by_conversation = data_processor.extract_categories_batch(processed_docs)
    
    # Add categories to processed documents
    for doc in processed_docs:
        doc_id = doc.get("_id")
        if doc_id in categories_by_conversation:
            doc["categories"] = categories_by_conversation[doc_id]
    
    # Store in MongoDB and/or Parquet if enabled
    if mongo_client or parquet_storage:
        # Get existing user analytics
        user_ids = set(doc.get("from_end_user_id") for doc in processed_docs if doc.get("from_end_user_id"))
        existing_user_analytics = {}
        
        for user_id in user_ids:
            user_doc = mongo_client.base_client.find_one(
                MONGODB_USER_ANALYTICS_COLLECTION,
                {"_id": user_id}
            )
            if user_doc:
                existing_user_analytics[user_id] = user_doc
        
        # Update user analytics
        updated_user_analytics = data_processor.update_user_analytics_batch(
            processed_docs,
            existing_user_analytics
        )
        
        # Generate analytics reports
        daily_reports = data_processor.generate_analytics_reports(processed_docs, "daily")
        weekly_reports = data_processor.generate_analytics_reports(processed_docs, "weekly")
        monthly_reports = data_processor.generate_analytics_reports(processed_docs, "monthly")
        
        # Store conversations in MongoDB
        if mongo_client:
            for doc in processed_docs:
                try:
                    # Use the conversation client to save the conversation
                    # This ensures messages are properly stored in the messages array
                    mongo_client.conversation.save_conversation(doc)
                    
                    # Update processing state
                    processing_state.update_last_processed(
                        conversation_id=doc["_id"],
                        timestamp=doc.get("created_at")
                    )
                    
                    processed_count += 1
                    last_processed_id = doc["_id"]
                    
                except Exception as e:
                    logging.error(f"Error storing conversation {doc.get('_id')}: {str(e)}")
                    processing_state.record_error(str(e), conversation_id=doc.get("_id"))
            
            # Store user analytics in MongoDB
            for user_doc in updated_user_analytics:
                try:
                    mongo_client.base_client.update_one(
                        MONGODB_USER_ANALYTICS_COLLECTION,
                        {"_id": user_doc["_id"]},
                        {"$set": user_doc},
                        upsert=True
                    )
                except Exception as e:
                    logging.error(f"Error storing user analytics for {user_doc.get('_id')}: {str(e)}")
            
            # Store analytics reports in MongoDB
            all_reports = daily_reports + weekly_reports + monthly_reports
            for report in all_reports:
                try:
                    mongo_client.base_client.update_one(
                        MONGODB_ANALYTICS_REPORTS_COLLECTION,
                        {"_id": report["_id"]},
                        {"$set": report},
                        upsert=True
                    )
                except Exception as e:
                    logging.error(f"Error storing analytics report {report.get('_id')}: {str(e)}")
        
        # Store data in Parquet format if enabled
        if parquet_storage:
            try:
                # Store conversations in Parquet format
                stored_paths = parquet_storage.store_conversations(processed_docs)
                logging.info(f"Stored conversations in Parquet format at: {', '.join(stored_paths)}")
                
                # Store user analytics in Parquet format
                user_analytics_path = parquet_storage.store_user_analytics(updated_user_analytics)
                if user_analytics_path:
                    logging.info(f"Stored user analytics in Parquet format at: {user_analytics_path}")
                
                # Store analytics reports in Parquet format
                all_reports = daily_reports + weekly_reports + monthly_reports
                report_paths = parquet_storage.store_analytics_reports(all_reports)
                if report_paths:
                    logging.info(f"Stored analytics reports in Parquet format at: {', '.join(report_paths.values())}")
                
                # Update processing state if MongoDB is not enabled
                if not mongo_client:
                    for doc in processed_docs:
                        processing_state.update_last_processed(
                            conversation_id=doc["_id"],
                            timestamp=doc.get("created_at")
                        )
                        
                        processed_count += 1
                        last_processed_id = doc["_id"]
            except Exception as e:
                logging.error(f"Error storing data in Parquet format: {str(e)}")
                processing_state.record_error(str(e))
    else:
        # Just update processing state
        for doc in processed_docs:
            processing_state.update_last_processed(
                conversation_id=doc["_id"],
                timestamp=doc.get("created_at")
            )
            
            processed_count += 1
            last_processed_id = doc["_id"]
    
    logging.info(f"Completed processing batch. Processed {processed_count}/{len(conversation_list)} conversations")
    return processed_count, last_processed_id


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Collect data from NocoDB and store in MongoDB/Parquet')
    parser.add_argument('--start-date', type=str, help='Start date for data collection (ISO format)')
    parser.add_argument('--end-date', type=str, help='End date for data collection (ISO format)')
    parser.add_argument('--app-id', type=str, help='App ID for filtering conversations')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help='Batch size for processing')
    parser.add_argument('--no-mongodb', action='store_true', help='Skip MongoDB storage')
    parser.add_argument('--no-parquet', action='store_true', help='Skip Parquet storage')
    parser.add_argument('--resume', action='store_true', help='Resume from last processed conversation')
    parser.add_argument('--state-file', type=str, default='processing_state.json', help='Path to state file')
    parser.add_argument('--use-s3-state', action='store_true', help='Use S3 for state tracking')
    args = parser.parse_args()
    
    # Set up logging
    setup_logging()
    
    # Validate configuration
    validate_config()
    
    # Initialize processing state
    processing_state = create_processing_state(
        use_s3=args.use_s3_state and S3_ENABLED,
        state_file_path=args.state_file,
        s3_bucket=S3_BUCKET,
        s3_key_prefix=f"{S3_PREFIX}/state"
    )
    
    # Start processing run
    processing_state.start_run()
    
    try:
        # Initialize clients
        nocodb_client = NocoDBClient()
        
        # Initialize storage clients
        mongo_client = None
        if not args.no_mongodb:
            mongo_client = MongoDBClient()
            create_mongodb_indexes(mongo_client)
        
        parquet_storage = None
        if PARQUET_STORAGE_ENABLED and not args.no_parquet:
            parquet_storage = ParquetStorage()
        
        # Initialize data processor
        data_processor = DataProcessor()
        
        # Get last processed ID if resuming
        last_id = None
        if args.resume:
            last_id = processing_state.get_last_processed_id()
            if last_id:
                logging.info(f"Resuming from conversation ID: {last_id}")
            else:
                logging.info("No previous state found, starting from the beginning")
        
        # Process data
        total_processed = 0
        
        while True:
            processed_count, last_id = process_conversation_batch(
                nocodb_client=nocodb_client,
                mongo_client=mongo_client,
                parquet_storage=parquet_storage,
                data_processor=data_processor,
                processing_state=processing_state,
                start_date=args.start_date,
                end_date=args.end_date,
                app_id=args.app_id,
                last_id=last_id,
                batch_size=args.batch_size
            )
            
            total_processed += processed_count
            
            # If no conversations were processed, we're done
            if processed_count == 0:
                break
        
        logging.info(f"Data collection completed. Processed {total_processed} conversations.")
        processing_state.end_run(success=True, message=f"Successfully processed {total_processed} conversations")
        
    except Exception as e:
        logging.error(f"Error in data collection: {str(e)}")
        processing_state.record_error(str(e))
        processing_state.end_run(success=False, message=f"Failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    main()

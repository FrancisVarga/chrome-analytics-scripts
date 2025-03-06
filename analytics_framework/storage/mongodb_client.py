"""Client for interacting with MongoDB."""

import logging
from typing import Dict, List, Any, Optional, Union
from pymongo import MongoClient, UpdateOne, InsertOne, IndexModel
from pymongo.errors import BulkWriteError, PyMongoError

from ..config import (
    MONGODB_URI,
    MONGODB_DATABASE,
    MONGODB_CONVERSATIONS_COLLECTION,
    MONGODB_TRANSLATIONS_COLLECTION,
    MONGODB_ANALYTICS_REPORTS_COLLECTION,
    MONGODB_USER_ANALYTICS_COLLECTION
)
from ..models.mongodb_schema import (
    CONVERSATION_ANALYTICS_INDEXES,
    CONVERSATION_TRANSLATIONS_INDEXES,
    ANALYTICS_REPORTS_INDEXES,
    USER_ANALYTICS_INDEXES
)


class MongoDBClient:
    """Client for interacting with MongoDB."""
    
    def __init__(
        self,
        uri: str = MONGODB_URI,
        database: str = MONGODB_DATABASE
    ):
        """
        Initialize the MongoDB client.
        
        Args:
            uri: MongoDB connection URI
            database: Database name
        """
        self.client = MongoClient(uri)
        self.db = self.client[database]
        self.logger = logging.getLogger(__name__)
        
        # Collection references
        self.conversations = self.db[MONGODB_CONVERSATIONS_COLLECTION]
        self.translations = self.db[MONGODB_TRANSLATIONS_COLLECTION]
        self.analytics_reports = self.db[MONGODB_ANALYTICS_REPORTS_COLLECTION]
        self.user_analytics = self.db[MONGODB_USER_ANALYTICS_COLLECTION]
        
        # Initialize indexes
        self._create_indexes()
    
    def _create_indexes(self):
        """Create indexes for all collections."""
        try:
            # Create indexes for conversations collection
            self._create_collection_indexes(
                self.conversations,
                CONVERSATION_ANALYTICS_INDEXES,
                "conversations"
            )
            
            # Create indexes for translations collection
            self._create_collection_indexes(
                self.translations,
                CONVERSATION_TRANSLATIONS_INDEXES,
                "translations"
            )
            
            # Create indexes for analytics reports collection
            self._create_collection_indexes(
                self.analytics_reports,
                ANALYTICS_REPORTS_INDEXES,
                "analytics_reports"
            )
            
            # Create indexes for user analytics collection
            self._create_collection_indexes(
                self.user_analytics,
                USER_ANALYTICS_INDEXES,
                "user_analytics"
            )
            
            self.logger.info("MongoDB indexes created successfully")
        except PyMongoError as e:
            self.logger.error(f"Error creating MongoDB indexes: {str(e)}")
    
    def _create_collection_indexes(self, collection, indexes, collection_name):
        """
        Create indexes for a collection.
        
        Args:
            collection: MongoDB collection
            indexes: List of index specifications
            collection_name: Name of the collection (for logging)
        """
        if not indexes:
            return
        
        try:
            # Convert index specifications to IndexModel objects
            index_models = [IndexModel(**index) for index in indexes]
            
            # Create indexes
            result = collection.create_indexes(index_models)
            
            self.logger.info(f"Created {len(result)} indexes for {collection_name} collection")
        except PyMongoError as e:
            self.logger.error(f"Error creating indexes for {collection_name}: {str(e)}")
    
    def insert_one(self, collection: str, document: Dict[str, Any]) -> str:
        """
        Insert a single document into a collection.
        
        Args:
            collection: Collection name
            document: Document to insert
            
        Returns:
            Inserted document ID
        """
        try:
            result = self.db[collection].insert_one(document)
            return str(result.inserted_id)
        except PyMongoError as e:
            self.logger.error(f"Error inserting document into {collection}: {str(e)}")
            raise
    
    def insert_many(self, collection: str, documents: List[Dict[str, Any]]) -> List[str]:
        """
        Insert multiple documents into a collection.
        
        Args:
            collection: Collection name
            documents: List of documents to insert
            
        Returns:
            List of inserted document IDs
        """
        if not documents:
            return []
            
        try:
            result = self.db[collection].insert_many(documents)
            return [str(id) for id in result.inserted_ids]
        except PyMongoError as e:
            self.logger.error(f"Error inserting documents into {collection}: {str(e)}")
            raise
    
    def bulk_write(
        self,
        collection: str,
        operations: List[Union[UpdateOne, InsertOne]]
    ) -> Dict[str, Any]:
        """
        Perform a bulk write operation.
        
        Args:
            collection: Collection name
            operations: List of write operations
            
        Returns:
            Result of the bulk write operation
        """
        if not operations:
            return {"acknowledged": True, "nModified": 0, "nUpserted": 0, "nMatched": 0}
            
        try:
            result = self.db[collection].bulk_write(operations, ordered=False)
            return {
                "acknowledged": result.acknowledged,
                "nModified": result.modified_count,
                "nUpserted": len(result.upserted_ids),
                "nMatched": result.matched_count
            }
        except BulkWriteError as bwe:
            self.logger.error(f"Bulk write error: {bwe.details}")
            # Return partial results
            return {
                "acknowledged": True,
                "nModified": bwe.details.get("nModified", 0),
                "nUpserted": len(bwe.details.get("upserted", [])),
                "nMatched": bwe.details.get("nMatched", 0),
                "writeErrors": len(bwe.details.get("writeErrors", []))
            }
        except PyMongoError as e:
            self.logger.error(f"Error performing bulk write on {collection}: {str(e)}")
            raise
    
    def find_one(
        self,
        collection: str,
        query: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Find a single document in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            projection: Fields to include/exclude
            
        Returns:
            Matching document or None
        """
        try:
            return self.db[collection].find_one(query, projection)
        except PyMongoError as e:
            self.logger.error(f"Error finding document in {collection}: {str(e)}")
            raise
    
    def find(
        self,
        collection: str,
        query: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[List[tuple]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Find documents in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            projection: Fields to include/exclude
            sort: Sorting criteria
            limit: Maximum number of documents to return
            skip: Number of documents to skip
            
        Returns:
            List of matching documents
        """
        try:
            cursor = self.db[collection].find(query, projection)
            
            if sort:
                cursor = cursor.sort(sort)
                
            if skip:
                cursor = cursor.skip(skip)
                
            if limit:
                cursor = cursor.limit(limit)
                
            return list(cursor)
        except PyMongoError as e:
            self.logger.error(f"Error finding documents in {collection}: {str(e)}")
            raise
    
    def update_one(
        self,
        collection: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False
    ) -> Dict[str, Any]:
        """
        Update a single document in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            update: Update operations
            upsert: Whether to insert if document doesn't exist
            
        Returns:
            Result of the update operation
        """
        try:
            result = self.db[collection].update_one(query, update, upsert=upsert)
            return {
                "acknowledged": result.acknowledged,
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
        except PyMongoError as e:
            self.logger.error(f"Error updating document in {collection}: {str(e)}")
            raise
    
    def update_many(
        self,
        collection: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False
    ) -> Dict[str, Any]:
        """
        Update multiple documents in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            update: Update operations
            upsert: Whether to insert if documents don't exist
            
        Returns:
            Result of the update operation
        """
        try:
            result = self.db[collection].update_many(query, update, upsert=upsert)
            return {
                "acknowledged": result.acknowledged,
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
        except PyMongoError as e:
            self.logger.error(f"Error updating documents in {collection}: {str(e)}")
            raise
    
    def delete_one(self, collection: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete a single document from a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            
        Returns:
            Result of the delete operation
        """
        try:
            result = self.db[collection].delete_one(query)
            return {
                "acknowledged": result.acknowledged,
                "deleted_count": result.deleted_count
            }
        except PyMongoError as e:
            self.logger.error(f"Error deleting document from {collection}: {str(e)}")
            raise
    
    def delete_many(self, collection: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete multiple documents from a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            
        Returns:
            Result of the delete operation
        """
        try:
            result = self.db[collection].delete_many(query)
            return {
                "acknowledged": result.acknowledged,
                "deleted_count": result.deleted_count
            }
        except PyMongoError as e:
            self.logger.error(f"Error deleting documents from {collection}: {str(e)}")
            raise
    
    def count_documents(self, collection: str, query: Dict[str, Any]) -> int:
        """
        Count documents in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            
        Returns:
            Number of matching documents
        """
        try:
            return self.db[collection].count_documents(query)
        except PyMongoError as e:
            self.logger.error(f"Error counting documents in {collection}: {str(e)}")
            raise
    
    def aggregate(
        self,
        collection: str,
        pipeline: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Perform an aggregation pipeline on a collection.
        
        Args:
            collection: Collection name
            pipeline: Aggregation pipeline
            
        Returns:
            Result of the aggregation
        """
        try:
            return list(self.db[collection].aggregate(pipeline))
        except PyMongoError as e:
            self.logger.error(f"Error performing aggregation on {collection}: {str(e)}")
            raise
    
    def close(self):
        """Close the MongoDB connection."""
        try:
            self.client.close()
            self.logger.info("MongoDB connection closed")
        except PyMongoError as e:
            self.logger.error(f"Error closing MongoDB connection: {str(e)}")

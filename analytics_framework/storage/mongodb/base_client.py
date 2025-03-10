"""Base MongoDB client with common operations."""

import logging
from typing import Dict, List, Any, Optional, Union, Tuple

from pymongo import MongoClient, UpdateOne, InsertOne, IndexModel
from pymongo.errors import BulkWriteError, PyMongoError, DuplicateKeyError
from pymongo.collection import Collection


class MongoDBBaseClient:
    """Base client for MongoDB operations with common functionality."""
    
    def __init__(
        self,
        uri: str,
        database: str,
        connect_timeout_ms: int = 30000,
        socket_timeout_ms: int = 30000,
        max_pool_size: int = 100
    ):
        """
        Initialize the MongoDB base client.
        
        Args:
            uri: MongoDB connection URI
            database: Database name
            connect_timeout_ms: Connection timeout in milliseconds
            socket_timeout_ms: Socket timeout in milliseconds
            max_pool_size: Maximum connection pool size
        """
        # Configure client with timeouts and connection pooling
        self.client = MongoClient(
            uri,
            connectTimeoutMS=connect_timeout_ms,
            socketTimeoutMS=socket_timeout_ms,
            maxPoolSize=max_pool_size,
            retryWrites=True
        )
        self.db = self.client[database]
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"MongoDB base client initialized for database: {database}")
    
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
            
            self.logger.info(
                f"Created {len(result)} indexes for {collection_name} collection"
            )
        except PyMongoError as e:
            self.logger.error(f"Error creating indexes for {collection_name}: {str(e)}")
    
    def ping(self) -> bool:
        """
        Check if the MongoDB connection is alive.
        
        Returns:
            True if the connection is alive, False otherwise
        """
        try:
            # The ping command will return True if the connection is alive
            return self.client.admin.command('ping')['ok'] == 1
        except PyMongoError as e:
            self.logger.error(f"MongoDB ping failed: {str(e)}")
            return False
    
    def get_collection(self, collection_name: str) -> Collection:
        """
        Get a collection by name.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            MongoDB collection
        """
        return self.db[collection_name]
    
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
        except DuplicateKeyError as e:
            self.logger.warning(
                f"Duplicate key error inserting document into {collection}: {str(e)}"
            )
            # Extract the duplicate key from the error message
            # This is a bit hacky, but PyMongo doesn't provide a clean way to get the duplicate key
            error_message = str(e)
            key_start = error_message.find('dup key: { ')
            if key_start != -1:
                key_info = error_message[key_start + 11:]
                key_end = key_info.find(' }')
                if key_end != -1:
                    key_info = key_info[:key_end]
                    self.logger.warning(f"Duplicate key: {key_info}")
            raise
        except PyMongoError as e:
            self.logger.error(f"Error inserting document into {collection}: {str(e)}")
            raise
    
    def insert_many(
        self, 
        collection: str, 
        documents: List[Dict[str, Any]], 
        ordered: bool = False
    ) -> List[str]:
        """
        Insert multiple documents into a collection.
        
        Args:
            collection: Collection name
            documents: List of documents to insert
            ordered: Whether to perform an ordered insert (stops on first error)
            
        Returns:
            List of inserted document IDs
        """
        if not documents:
            return []
            
        try:
            result = self.db[collection].insert_many(documents, ordered=ordered)
            return [str(id) for id in result.inserted_ids]
        except BulkWriteError as bwe:
            # Handle partial success in bulk write operations
            self.logger.warning(
                f"Bulk write error inserting documents into {collection}: {bwe.details}"
            )
            # Extract successfully inserted IDs
            inserted_ids = []
            if hasattr(bwe, 'details') and 'writeErrors' in bwe.details:
                # Calculate which documents were successfully inserted
                error_indices = [err['index'] for err in bwe.details['writeErrors']]
                for i, doc in enumerate(documents):
                    if i not in error_indices and '_id' in doc:
                        inserted_ids.append(str(doc['_id']))
            
            self.logger.info(
                f"Inserted {len(inserted_ids)} documents successfully out of {len(documents)}"
            )
            
            # Re-raise the exception for the caller to handle
            raise
        except PyMongoError as e:
            self.logger.error(f"Error inserting documents into {collection}: {str(e)}")
            raise
    
    def bulk_write(
        self,
        collection: str,
        operations: List[Union[UpdateOne, InsertOne]],
        ordered: bool = False,
        bypass_document_validation: bool = False
    ) -> Dict[str, Any]:
        """
        Perform a bulk write operation.
        
        Args:
            collection: Collection name
            operations: List of write operations
            ordered: Whether to perform an ordered operation (stops on first error)
            bypass_document_validation: Whether to bypass document validation
            
        Returns:
            Result of the bulk write operation
        """
        if not operations:
            return {"acknowledged": True, "nModified": 0, "nUpserted": 0, "nMatched": 0}
            
        try:
            result = self.db[collection].bulk_write(
                operations, 
                ordered=ordered,
                bypass_document_validation=bypass_document_validation
            )
            return {
                "acknowledged": result.acknowledged,
                "nModified": result.modified_count,
                "nUpserted": len(result.upserted_ids),
                "nMatched": result.matched_count,
                "nInserted": result.inserted_count,
                "nRemoved": result.deleted_count
            }
        except BulkWriteError as bwe:
            self.logger.warning(f"Bulk write error: {bwe.details}")
            # Return partial results
            return {
                "acknowledged": True,
                "nModified": bwe.details.get("nModified", 0),
                "nUpserted": len(bwe.details.get("upserted", [])),
                "nMatched": bwe.details.get("nMatched", 0),
                "nInserted": bwe.details.get("nInserted", 0),
                "nRemoved": bwe.details.get("nRemoved", 0),
                "writeErrors": len(bwe.details.get("writeErrors", [])),
                "error": str(bwe)
            }
        except PyMongoError as e:
            self.logger.error(f"Error performing bulk write on {collection}: {str(e)}")
            raise
    
    def find_one(
        self,
        collection: str,
        query: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[List[Tuple[str, int]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Find a single document in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            projection: Fields to include/exclude
            sort: Sorting criteria
            
        Returns:
            Matching document or None
        """
        try:
            if sort:
                return self.db[collection].find_one(query, projection, sort=sort)
            else:
                return self.db[collection].find_one(query, projection)
        except PyMongoError as e:
            self.logger.error(f"Error finding document in {collection}: {str(e)}")
            raise
    
    def find(
        self,
        collection: str,
        query: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        batch_size: Optional[int] = None
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
            batch_size: Number of documents to return in each batch
            
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
                
            if batch_size:
                cursor = cursor.batch_size(batch_size)
                
            return list(cursor)
        except PyMongoError as e:
            self.logger.error(f"Error finding documents in {collection}: {str(e)}")
            raise
    
    def find_with_cursor(
        self,
        collection: str,
        query: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        batch_size: int = 100
    ):
        """
        Find documents in a collection and return a cursor for iteration.
        Useful for processing large result sets without loading everything into memory.
        
        Args:
            collection: Collection name
            query: Query filter
            projection: Fields to include/exclude
            sort: Sorting criteria
            batch_size: Number of documents to return in each batch
            
        Returns:
            Cursor for iteration
        """
        try:
            cursor = self.db[collection].find(query, projection)
            
            if sort:
                cursor = cursor.sort(sort)
                
            return cursor.batch_size(batch_size)
        except PyMongoError as e:
            self.logger.error(f"Error creating cursor for {collection}: {str(e)}")
            raise
    
    def update_one(
        self,
        collection: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
        bypass_document_validation: bool = False
    ) -> Dict[str, Any]:
        """
        Update a single document in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            update: Update operations
            upsert: Whether to insert if document doesn't exist
            bypass_document_validation: Whether to bypass document validation
            
        Returns:
            Result of the update operation
        """
        try:
            result = self.db[collection].update_one(
                query, 
                update, 
                upsert=upsert,
                bypass_document_validation=bypass_document_validation
            )
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
        upsert: bool = False,
        bypass_document_validation: bool = False
    ) -> Dict[str, Any]:
        """
        Update multiple documents in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            update: Update operations
            upsert: Whether to insert if documents don't exist
            bypass_document_validation: Whether to bypass document validation
            
        Returns:
            Result of the update operation
        """
        try:
            result = self.db[collection].update_many(
                query, 
                update, 
                upsert=upsert,
                bypass_document_validation=bypass_document_validation
            )
            return {
                "acknowledged": result.acknowledged,
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
        except PyMongoError as e:
            self.logger.error(f"Error updating documents in {collection}: {str(e)}")
            raise
    
    def replace_one(
        self,
        collection: str,
        query: Dict[str, Any],
        replacement: Dict[str, Any],
        upsert: bool = False,
        bypass_document_validation: bool = False
    ) -> Dict[str, Any]:
        """
        Replace a single document in a collection.
        
        Args:
            collection: Collection name
            query: Query filter
            replacement: Replacement document
            upsert: Whether to insert if document doesn't exist
            bypass_document_validation: Whether to bypass document validation
            
        Returns:
            Result of the replace operation
        """
        try:
            result = self.db[collection].replace_one(
                query, 
                replacement, 
                upsert=upsert,
                bypass_document_validation=bypass_document_validation
            )
            return {
                "acknowledged": result.acknowledged,
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
        except PyMongoError as e:
            self.logger.error(f"Error replacing document in {collection}: {str(e)}")
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
    
    def distinct(self, collection: str, field: str, query: Dict[str, Any] = None) -> List[Any]:
        """
        Get distinct values for a field in a collection.
        
        Args:
            collection: Collection name
            field: Field name
            query: Query filter
            
        Returns:
            List of distinct values
        """
        try:
            return self.db[collection].distinct(field, query)
        except PyMongoError as e:
            self.logger.error(f"Error getting distinct values from {collection}: {str(e)}")
            raise
    
    def create_index(self, collection: str, keys: List[Tuple[str, int]], **kwargs) -> str:
        """
        Create an index on a collection.
        
        Args:
            collection: Collection name
            keys: List of (field, direction) pairs
            **kwargs: Additional index options
            
        Returns:
            Name of the created index
        """
        try:
            return self.db[collection].create_index(keys, **kwargs)
        except PyMongoError as e:
            self.logger.error(f"Error creating index on {collection}: {str(e)}")
            raise
    
    def close(self):
        """Close the MongoDB connection."""
        try:
            self.client.close()
            self.logger.info("MongoDB connection closed")
        except PyMongoError as e:
            self.logger.error(f"Error closing MongoDB connection: {str(e)}")

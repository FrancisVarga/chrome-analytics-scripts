"""Main MongoDB client that combines all specialized clients."""

import logging

from ..mongodb.base_client import MongoDBBaseClient
from ..mongodb.conversation_client import MongoDBConversationClient
from ..mongodb.analytics_client import MongoDBAnalyticsClient
from ..mongodb.translation_client import MongoDBTranslationClient
from ...config import MONGODB_URI, MONGODB_DATABASE


class MongoDBClient:
    """Main MongoDB client that combines all specialized clients."""
    
    def __init__(
        self,
        uri: str = MONGODB_URI,
        database: str = MONGODB_DATABASE,
        connect_timeout_ms: int = 30000,
        socket_timeout_ms: int = 30000,
        max_pool_size: int = 100
    ):
        """
        Initialize the MongoDB client.
        
        Args:
            uri: MongoDB connection URI
            database: Database name
            connect_timeout_ms: Connection timeout in milliseconds
            socket_timeout_ms: Socket timeout in milliseconds
            max_pool_size: Maximum connection pool size
        """
        self.logger = logging.getLogger(__name__)
        
        # Create base client
        self.base_client = MongoDBBaseClient(
            uri=uri,
            database=database,
            connect_timeout_ms=connect_timeout_ms,
            socket_timeout_ms=socket_timeout_ms,
            max_pool_size=max_pool_size
        )
        
        # Create specialized clients
        self.conversation = MongoDBConversationClient(self.base_client)
        self.analytics = MongoDBAnalyticsClient(self.base_client)
        self.translation = MongoDBTranslationClient(self.base_client)
        
        self.logger.info(f"MongoDB client initialized for database: {database}")
    
    def ping(self) -> bool:
        """
        Check if the MongoDB connection is alive.
        
        Returns:
            True if the connection is alive, False otherwise
        """
        return self.base_client.ping()
    
    def close(self):
        """Close the MongoDB connection."""
        self.base_client.close()

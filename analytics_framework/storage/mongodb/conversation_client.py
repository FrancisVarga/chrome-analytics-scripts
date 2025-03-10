"""MongoDB client for conversation operations."""

import logging
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional

from ..mongodb.base_client import MongoDBBaseClient
from ...config import MONGODB_CONVERSATIONS_COLLECTION


class MongoDBConversationClient:
    """Client for MongoDB conversation operations."""
    
    def __init__(self, base_client: MongoDBBaseClient):
        """
        Initialize the MongoDB conversation client.
        
        Args:
            base_client: Base MongoDB client
        """
        self.base_client = base_client
        self.collection = MONGODB_CONVERSATIONS_COLLECTION
        self.logger = logging.getLogger(__name__)
    
    def save_conversation(self, conversation_data: Dict[str, Any]) -> str:
        """
        Save a conversation to the conversations collection.
        
        Args:
            conversation_data: Conversation data
            
        Returns:
            Conversation ID
        """
        try:
            # Ensure _id is present
            if "_id" not in conversation_data:
                conversation_data["_id"] = conversation_data.get("id", str(uuid.uuid4()))
            
            # Set timestamps if not present
            now = datetime.now().isoformat()
            if "created_at" not in conversation_data:
                conversation_data["created_at"] = now
            if "updated_at" not in conversation_data:
                conversation_data["updated_at"] = now
            
            # Insert or replace the conversation
            self.base_client.replace_one(
                self.collection,
                {"_id": conversation_data["_id"]},
                conversation_data,
                upsert=True
            )
            
            return conversation_data["_id"]
        except Exception as e:
            self.logger.error(f"Error saving conversation: {str(e)}")
            raise
    
    def get_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a conversation by ID.
        
        Args:
            conversation_id: Conversation ID
            
        Returns:
            Conversation data or None if not found
        """
        return self.base_client.find_one(self.collection, {"_id": conversation_id})
    
    def get_conversations_by_user(
        self,
        user_id: str,
        limit: int = 100,
        skip: int = 0,
        sort_by: str = "created_at",
        sort_direction: int = -1
    ) -> List[Dict[str, Any]]:
        """
        Get conversations for a user.
        
        Args:
            user_id: User ID
            limit: Maximum number of conversations to return
            skip: Number of conversations to skip
            sort_by: Field to sort by
            sort_direction: Sort direction (1 for ascending, -1 for descending)
            
        Returns:
            List of conversations
        """
        return self.base_client.find(
            self.collection,
            {"from_end_user_id": user_id},
            sort=[(sort_by, sort_direction)],
            limit=limit,
            skip=skip
        )
    
    def get_conversations_by_app(
        self,
        app_id: str,
        limit: int = 100,
        skip: int = 0,
        sort_by: str = "created_at",
        sort_direction: int = -1
    ) -> List[Dict[str, Any]]:
        """
        Get conversations for an app.
        
        Args:
            app_id: App ID
            limit: Maximum number of conversations to return
            skip: Number of conversations to skip
            sort_by: Field to sort by
            sort_direction: Sort direction (1 for ascending, -1 for descending)
            
        Returns:
            List of conversations
        """
        return self.base_client.find(
            self.collection,
            {"app_id": app_id},
            sort=[(sort_by, sort_direction)],
            limit=limit,
            skip=skip
        )
    
    def get_conversations_by_model(
        self,
        model_id: str,
        limit: int = 100,
        skip: int = 0,
        sort_by: str = "created_at",
        sort_direction: int = -1
    ) -> List[Dict[str, Any]]:
        """
        Get conversations for a model.
        
        Args:
            model_id: Model ID
            limit: Maximum number of conversations to return
            skip: Number of conversations to skip
            sort_by: Field to sort by
            sort_direction: Sort direction (1 for ascending, -1 for descending)
            
        Returns:
            List of conversations
        """
        return self.base_client.find(
            self.collection,
            {"model_id": model_id},
            sort=[(sort_by, sort_direction)],
            limit=limit,
            skip=skip
        )
    
    def get_conversations_by_category(
        self,
        category_type: str,
        category_value: str,
        limit: int = 100,
        skip: int = 0,
        sort_by: str = "created_at",
        sort_direction: int = -1
    ) -> List[Dict[str, Any]]:
        """
        Get conversations by category.
        
        Args:
            category_type: Category type
            category_value: Category value
            limit: Maximum number of conversations to return
            skip: Number of conversations to skip
            sort_by: Field to sort by
            sort_direction: Sort direction (1 for ascending, -1 for descending)
            
        Returns:
            List of conversations
        """
        return self.base_client.find(
            self.collection,
            {
                "categories.category_type": category_type,
                "categories.category_value": category_value
            },
            sort=[(sort_by, sort_direction)],
            limit=limit,
            skip=skip
        )
    
    def get_conversations_by_date_range(
        self,
        start_date: str,
        end_date: str,
        limit: int = 100,
        skip: int = 0,
        sort_by: str = "created_at",
        sort_direction: int = -1
    ) -> List[Dict[str, Any]]:
        """
        Get conversations within a date range.
        
        Args:
            start_date: Start date (ISO format)
            end_date: End date (ISO format)
            limit: Maximum number of conversations to return
            skip: Number of conversations to skip
            sort_by: Field to sort by
            sort_direction: Sort direction (1 for ascending, -1 for descending)
            
        Returns:
            List of conversations
        """
        return self.base_client.find(
            self.collection,
            {
                "created_at": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            },
            sort=[(sort_by, sort_direction)],
            limit=limit,
            skip=skip
        )
    
    def add_category_to_conversation(
        self,
        conversation_id: str,
        category_type: str,
        category_value: str,
        confidence_score: float = 1.0,
        created_by: str = "system"
    ) -> Dict[str, Any]:
        """
        Add a category to a conversation.
        
        Args:
            conversation_id: Conversation ID
            category_type: Category type
            category_value: Category value
            confidence_score: Confidence score
            created_by: Entity that created the category
            
        Returns:
            Result of the update operation
        """
        category_id = str(uuid.uuid4())
        created_at = datetime.now().isoformat()
        
        category = {
            "category_id": category_id,
            "category_type": category_type,
            "category_value": category_value,
            "confidence_score": confidence_score,
            "created_at": created_at,
            "created_by": created_by
        }
        
        return self.base_client.update_one(
            self.collection,
            {"_id": conversation_id},
            {"$push": {"categories": category}}
        )
    
    def remove_category_from_conversation(
        self,
        conversation_id: str,
        category_id: str
    ) -> Dict[str, Any]:
        """
        Remove a category from a conversation.
        
        Args:
            conversation_id: Conversation ID
            category_id: Category ID
            
        Returns:
            Result of the update operation
        """
        return self.base_client.update_one(
            self.collection,
            {"_id": conversation_id},
            {"$pull": {"categories": {"category_id": category_id}}}
        )
    
    def add_message_to_conversation(
        self,
        conversation_id: str,
        message_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Add a message to a conversation.
        
        Args:
            conversation_id: Conversation ID
            message_data: Message data
            
        Returns:
            Result of the update operation
        """
        # Ensure message_id is present
        if "message_id" not in message_data:
            message_data["message_id"] = str(uuid.uuid4())
        
        # Set created_at if not present
        if "created_at" not in message_data:
            message_data["created_at"] = datetime.now().isoformat()
        
        return self.base_client.update_one(
            self.collection,
            {"_id": conversation_id},
            {
                "$push": {"messages": message_data},
                "$inc": {"message_count": 1},
                "$set": {"updated_at": datetime.now().isoformat()}
            }
        )
    
    def update_conversation_metrics(
        self,
        conversation_id: str,
        total_tokens: int = 0,
        total_price: float = 0.0
    ) -> Dict[str, Any]:
        """
        Update conversation metrics.
        
        Args:
            conversation_id: Conversation ID
            total_tokens: Total tokens to add
            total_price: Total price to add
            
        Returns:
            Result of the update operation
        """
        update = {
            "$inc": {},
            "$set": {"updated_at": datetime.now().isoformat()}
        }
        
        if total_tokens > 0:
            update["$inc"]["total_tokens"] = total_tokens
        
        if total_price > 0:
            update["$inc"]["total_price"] = total_price
        
        return self.base_client.update_one(
            self.collection,
            {"_id": conversation_id},
            update
        )
    
    def update_conversation_status(
        self,
        conversation_id: str,
        status: str
    ) -> Dict[str, Any]:
        """
        Update conversation status.
        
        Args:
            conversation_id: Conversation ID
            status: New status
            
        Returns:
            Result of the update operation
        """
        return self.base_client.update_one(
            self.collection,
            {"_id": conversation_id},
            {
                "$set": {
                    "status": status,
                    "updated_at": datetime.now().isoformat()
                }
            }
        )
    
    def delete_conversation(self, conversation_id: str) -> Dict[str, Any]:
        """
        Delete a conversation.
        
        Args:
            conversation_id: Conversation ID
            
        Returns:
            Result of the delete operation
        """
        return self.base_client.delete_one(self.collection, {"_id": conversation_id})
    
    def mark_conversation_deleted(self, conversation_id: str) -> Dict[str, Any]:
        """
        Mark a conversation as deleted (soft delete).
        
        Args:
            conversation_id: Conversation ID
            
        Returns:
            Result of the update operation
        """
        return self.base_client.update_one(
            self.collection,
            {"_id": conversation_id},
            {
                "$set": {
                    "is_deleted": True,
                    "updated_at": datetime.now().isoformat()
                }
            }
        )
    
    def get_conversation_count_by_user(self, user_id: str) -> int:
        """
        Get the number of conversations for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            Number of conversations
        """
        return self.base_client.count_documents(
            self.collection,
            {"from_end_user_id": user_id}
        )
    
    def get_conversation_count_by_app(self, app_id: str) -> int:
        """
        Get the number of conversations for an app.
        
        Args:
            app_id: App ID
            
        Returns:
            Number of conversations
        """
        return self.base_client.count_documents(
            self.collection,
            {"app_id": app_id}
        )
    
    def get_conversation_count_by_model(self, model_id: str) -> int:
        """
        Get the number of conversations for a model.
        
        Args:
            model_id: Model ID
            
        Returns:
            Number of conversations
        """
        return self.base_client.count_documents(
            self.collection,
            {"model_id": model_id}
        )
    
    def get_conversation_count_by_date_range(
        self,
        start_date: str,
        end_date: str
    ) -> int:
        """
        Get the number of conversations within a date range.
        
        Args:
            start_date: Start date (ISO format)
            end_date: End date (ISO format)
            
        Returns:
            Number of conversations
        """
        return self.base_client.count_documents(
            self.collection,
            {
                "created_at": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }
        )

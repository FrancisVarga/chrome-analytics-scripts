"""MongoDB client for translation operations."""

import logging
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional

from ..mongodb.base_client import MongoDBBaseClient
from ...config import MONGODB_TRANSLATIONS_COLLECTION


class MongoDBTranslationClient:
    """Client for MongoDB translation operations."""
    
    def __init__(self, base_client: MongoDBBaseClient):
        """
        Initialize the MongoDB translation client.
        
        Args:
            base_client: Base MongoDB client
        """
        self.base_client = base_client
        self.collection = MONGODB_TRANSLATIONS_COLLECTION
        self.logger = logging.getLogger(__name__)
    
    def save_translation(self, translation_data: Dict[str, Any]) -> str:
        """
        Save a translation to the translations collection.
        
        Args:
            translation_data: Translation data
            
        Returns:
            Translation ID
        """
        try:
            # Ensure _id is present
            if "_id" not in translation_data:
                translation_data["_id"] = translation_data.get(
                    "id", str(uuid.uuid4())
                )
            
            # Set timestamps if not present
            now = datetime.now().isoformat()
            if "created_at" not in translation_data:
                translation_data["created_at"] = now
            if "updated_at" not in translation_data:
                translation_data["updated_at"] = now
            
            # Insert or replace the translation
            self.base_client.replace_one(
                self.collection,
                {"_id": translation_data["_id"]},
                translation_data,
                upsert=True
            )
            
            return translation_data["_id"]
        except Exception as e:
            self.logger.error(f"Error saving translation: {str(e)}")
            raise
    
    def get_translation(self, translation_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a translation by ID.
        
        Args:
            translation_id: Translation ID
            
        Returns:
            Translation data or None if not found
        """
        return self.base_client.find_one(self.collection, {"_id": translation_id})
    
    def get_translations_by_conversation(
        self,
        conversation_id: str,
        language_code: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get translations for a conversation.
        
        Args:
            conversation_id: Conversation ID
            language_code: Language code (optional)
            
        Returns:
            List of translations
        """
        query = {"conversation_id": conversation_id}
        
        if language_code:
            query["language_code"] = language_code
        
        return self.base_client.find(
            self.collection,
            query,
            sort=[("created_at", 1)]
        )
    
    def get_translations_by_message(
        self,
        message_id: str,
        language_code: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get translations for a message.
        
        Args:
            message_id: Message ID
            language_code: Language code (optional)
            
        Returns:
            List of translations
        """
        query = {"message_id": message_id}
        
        if language_code:
            query["language_code"] = language_code
        
        return self.base_client.find(
            self.collection,
            query,
            sort=[("created_at", 1)]
        )
    
    def get_translations_by_language(
        self,
        language_code: str,
        limit: int = 100,
        skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get translations by language.
        
        Args:
            language_code: Language code
            limit: Maximum number of translations to return
            skip: Number of translations to skip
            
        Returns:
            List of translations
        """
        return self.base_client.find(
            self.collection,
            {"language_code": language_code},
            sort=[("created_at", -1)],
            limit=limit,
            skip=skip
        )
    
    def update_translation_content(
        self,
        translation_id: str,
        translated_content: str
    ) -> Dict[str, Any]:
        """
        Update translation content.
        
        Args:
            translation_id: Translation ID
            translated_content: New translated content
            
        Returns:
            Result of the update operation
        """
        return self.base_client.update_one(
            self.collection,
            {"_id": translation_id},
            {
                "$set": {
                    "translated_content": translated_content,
                    "updated_at": datetime.now().isoformat()
                }
            }
        )
    
    def delete_translation(self, translation_id: str) -> Dict[str, Any]:
        """
        Delete a translation.
        
        Args:
            translation_id: Translation ID
            
        Returns:
            Result of the delete operation
        """
        return self.base_client.delete_one(self.collection, {"_id": translation_id})
    
    def delete_translations_by_conversation(
        self,
        conversation_id: str,
        language_code: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete translations for a conversation.
        
        Args:
            conversation_id: Conversation ID
            language_code: Language code (optional)
            
        Returns:
            Result of the delete operation
        """
        query = {"conversation_id": conversation_id}
        
        if language_code:
            query["language_code"] = language_code
        
        return self.base_client.delete_many(self.collection, query)
    
    def delete_translations_by_message(
        self,
        message_id: str,
        language_code: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete translations for a message.
        
        Args:
            message_id: Message ID
            language_code: Language code (optional)
            
        Returns:
            Result of the delete operation
        """
        query = {"message_id": message_id}
        
        if language_code:
            query["language_code"] = language_code
        
        return self.base_client.delete_many(self.collection, query)
    
    def get_available_languages(
        self,
        conversation_id: Optional[str] = None
    ) -> List[str]:
        """
        Get available languages.
        
        Args:
            conversation_id: Conversation ID (optional)
            
        Returns:
            List of language codes
        """
        query = {}
        
        if conversation_id:
            query["conversation_id"] = conversation_id
        
        return self.base_client.distinct(self.collection, "language_code", query)
    
    def get_translation_count_by_language(self) -> List[Dict[str, Any]]:
        """
        Get translation count by language.
        
        Returns:
            List of language codes with their counts
        """
        pipeline = [
            {"$group": {"_id": "$language_code", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        return self.base_client.aggregate(self.collection, pipeline)
    
    def get_translation_count_by_conversation(
        self,
        conversation_id: str
    ) -> int:
        """
        Get translation count for a conversation.
        
        Args:
            conversation_id: Conversation ID
            
        Returns:
            Number of translations
        """
        return self.base_client.count_documents(
            self.collection,
            {"conversation_id": conversation_id}
        )

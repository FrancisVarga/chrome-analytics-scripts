"""MongoDB client for analytics operations."""

import logging
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional

from ..mongodb.base_client import MongoDBBaseClient
from ...config import (
    MONGODB_ANALYTICS_REPORTS_COLLECTION,
    MONGODB_USER_ANALYTICS_COLLECTION
)


class MongoDBAnalyticsClient:
    """Client for MongoDB analytics operations."""
    
    def __init__(self, base_client: MongoDBBaseClient):
        """
        Initialize the MongoDB analytics client.
        
        Args:
            base_client: Base MongoDB client
        """
        self.base_client = base_client
        self.reports_collection = MONGODB_ANALYTICS_REPORTS_COLLECTION
        self.user_analytics_collection = MONGODB_USER_ANALYTICS_COLLECTION
        self.logger = logging.getLogger(__name__)
    
    # Analytics Reports Methods
    
    def save_analytics_report(self, report_data: Dict[str, Any]) -> str:
        """
        Save an analytics report.
        
        Args:
            report_data: Report data
            
        Returns:
            Report ID
        """
        try:
            # Ensure _id is present
            if "_id" not in report_data:
                report_data["_id"] = report_data.get("id", str(uuid.uuid4()))
            
            # Set created_at if not present
            if "created_at" not in report_data:
                report_data["created_at"] = datetime.now().isoformat()
            
            # Insert or replace the report
            self.base_client.replace_one(
                self.reports_collection,
                {"_id": report_data["_id"]},
                report_data,
                upsert=True
            )
            
            return report_data["_id"]
        except Exception as e:
            self.logger.error(f"Error saving analytics report: {str(e)}")
            raise
    
    def get_analytics_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an analytics report by ID.
        
        Args:
            report_id: Report ID
            
        Returns:
            Report data or None if not found
        """
        return self.base_client.find_one(
            self.reports_collection,
            {"_id": report_id}
        )
    
    def get_analytics_reports_by_type(
        self,
        report_type: str,
        limit: int = 100,
        skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get analytics reports by type.
        
        Args:
            report_type: Report type
            limit: Maximum number of reports to return
            skip: Number of reports to skip
            
        Returns:
            List of reports
        """
        return self.base_client.find(
            self.reports_collection,
            {"report_type": report_type},
            sort=[("period_end", -1)],
            limit=limit,
            skip=skip
        )
    
    def get_analytics_reports_by_period(
        self,
        start_date: str,
        end_date: str,
        report_type: Optional[str] = None,
        limit: int = 100,
        skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get analytics reports by period.
        
        Args:
            start_date: Start date (ISO format)
            end_date: End date (ISO format)
            report_type: Report type (optional)
            limit: Maximum number of reports to return
            skip: Number of reports to skip
            
        Returns:
            List of reports
        """
        query = {
            "period_start": {"$gte": start_date},
            "period_end": {"$lte": end_date}
        }
        
        if report_type:
            query["report_type"] = report_type
        
        return self.base_client.find(
            self.reports_collection,
            query,
            sort=[("period_end", -1)],
            limit=limit,
            skip=skip
        )
    
    def delete_analytics_report(self, report_id: str) -> Dict[str, Any]:
        """
        Delete an analytics report.
        
        Args:
            report_id: Report ID
            
        Returns:
            Result of the delete operation
        """
        return self.base_client.delete_one(
            self.reports_collection,
            {"_id": report_id}
        )
    
    # User Analytics Methods
    
    def save_user_analytics(self, user_analytics_data: Dict[str, Any]) -> str:
        """
        Save user analytics.
        
        Args:
            user_analytics_data: User analytics data
            
        Returns:
            User ID
        """
        try:
            # Ensure _id is present
            if "_id" not in user_analytics_data:
                user_analytics_data["_id"] = user_analytics_data.get(
                    "id", str(uuid.uuid4())
                )
            
            # Set updated_at
            user_analytics_data["updated_at"] = datetime.now().isoformat()
            
            # Insert or replace the user analytics
            self.base_client.replace_one(
                self.user_analytics_collection,
                {"_id": user_analytics_data["_id"]},
                user_analytics_data,
                upsert=True
            )
            
            return user_analytics_data["_id"]
        except Exception as e:
            self.logger.error(f"Error saving user analytics: {str(e)}")
            raise
    
    def get_user_analytics(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get user analytics by user ID.
        
        Args:
            user_id: User ID
            
        Returns:
            User analytics data or None if not found
        """
        return self.base_client.find_one(
            self.user_analytics_collection,
            {"_id": user_id}
        )
    
    def update_user_analytics_metrics(
        self,
        user_id: str,
        total_conversations: int = 0,
        total_messages: int = 0,
        total_tokens: int = 0,
        total_price: float = 0.0,
        last_conversation_at: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update user analytics metrics.
        
        Args:
            user_id: User ID
            total_conversations: Number of conversations to add
            total_messages: Number of messages to add
            total_tokens: Number of tokens to add
            total_price: Price to add
            last_conversation_at: Timestamp of the last conversation
            
        Returns:
            Result of the update operation
        """
        update = {
            "$inc": {},
            "$set": {"updated_at": datetime.now().isoformat()}
        }
        
        if total_conversations > 0:
            update["$inc"]["total_conversations"] = total_conversations
        
        if total_messages > 0:
            update["$inc"]["total_messages"] = total_messages
        
        if total_tokens > 0:
            update["$inc"]["total_tokens"] = total_tokens
        
        if total_price > 0:
            update["$inc"]["total_price"] = total_price
        
        if last_conversation_at:
            update["$set"]["last_conversation_at"] = last_conversation_at
            
            # Set first_conversation_at if not already set
            first_conversation = self.base_client.find_one(
                self.user_analytics_collection,
                {"_id": user_id, "first_conversation_at": {"$exists": False}}
            )
            
            if first_conversation:
                update["$set"]["first_conversation_at"] = last_conversation_at
        
        return self.base_client.update_one(
            self.user_analytics_collection,
            {"_id": user_id},
            update,
            upsert=True
        )
    
    def update_user_category_distribution(
        self,
        user_id: str,
        category_type: str,
        category_value: str
    ) -> Dict[str, Any]:
        """
        Update user category distribution.
        
        Args:
            user_id: User ID
            category_type: Category type
            category_value: Category value
            
        Returns:
            Result of the update operation
        """
        category_key = f"category_distribution.{category_type}.{category_value}"
        
        return self.base_client.update_one(
            self.user_analytics_collection,
            {"_id": user_id},
            {
                "$inc": {category_key: 1},
                "$set": {"updated_at": datetime.now().isoformat()}
            },
            upsert=True
        )
    
    def update_user_model_usage(
        self,
        user_id: str,
        model_id: str,
        tokens: int = 0,
        price: float = 0.0,
        conversations: int = 0
    ) -> Dict[str, Any]:
        """
        Update user model usage.
        
        Args:
            user_id: User ID
            model_id: Model ID
            tokens: Number of tokens to add
            price: Price to add
            conversations: Number of conversations to add
            
        Returns:
            Result of the update operation
        """
        update = {
            "$set": {"updated_at": datetime.now().isoformat()}
        }
        
        if tokens > 0:
            update["$inc"] = update.get("$inc", {})
            update["$inc"][f"model_usage.{model_id}.tokens"] = tokens
        
        if price > 0:
            update["$inc"] = update.get("$inc", {})
            update["$inc"][f"model_usage.{model_id}.price"] = price
        
        if conversations > 0:
            update["$inc"] = update.get("$inc", {})
            update["$inc"][f"model_usage.{model_id}.conversations"] = conversations
        
        return self.base_client.update_one(
            self.user_analytics_collection,
            {"_id": user_id},
            update,
            upsert=True
        )
    
    def update_user_daily_metrics(
        self,
        user_id: str,
        date: str,
        conversations: int = 0,
        messages: int = 0,
        tokens: int = 0,
        price: float = 0.0
    ) -> Dict[str, Any]:
        """
        Update user daily metrics.
        
        Args:
            user_id: User ID
            date: Date in YYYY-MM-DD format
            conversations: Number of conversations to add
            messages: Number of messages to add
            tokens: Number of tokens to add
            price: Price to add
            
        Returns:
            Result of the update operation
        """
        update = {
            "$set": {"updated_at": datetime.now().isoformat()}
        }
        
        if conversations > 0:
            update["$inc"] = update.get("$inc", {})
            update["$inc"][f"daily_metrics.{date}.conversations"] = conversations
        
        if messages > 0:
            update["$inc"] = update.get("$inc", {})
            update["$inc"][f"daily_metrics.{date}.messages"] = messages
        
        if tokens > 0:
            update["$inc"] = update.get("$inc", {})
            update["$inc"][f"daily_metrics.{date}.tokens"] = tokens
        
        if price > 0:
            update["$inc"] = update.get("$inc", {})
            update["$inc"][f"daily_metrics.{date}.price"] = price
        
        return self.base_client.update_one(
            self.user_analytics_collection,
            {"_id": user_id},
            update,
            upsert=True
        )
    
    def get_top_users_by_conversations(
        self,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top users by number of conversations.
        
        Args:
            limit: Maximum number of users to return
            
        Returns:
            List of users with their conversation counts
        """
        return self.base_client.find(
            self.user_analytics_collection,
            {},
            projection={"_id": 1, "total_conversations": 1},
            sort=[("total_conversations", -1)],
            limit=limit
        )
    
    def get_top_users_by_tokens(
        self,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top users by number of tokens.
        
        Args:
            limit: Maximum number of users to return
            
        Returns:
            List of users with their token counts
        """
        return self.base_client.find(
            self.user_analytics_collection,
            {},
            projection={"_id": 1, "total_tokens": 1},
            sort=[("total_tokens", -1)],
            limit=limit
        )
    
    def get_top_users_by_price(
        self,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top users by price.
        
        Args:
            limit: Maximum number of users to return
            
        Returns:
            List of users with their price totals
        """
        return self.base_client.find(
            self.user_analytics_collection,
            {},
            projection={"_id": 1, "total_price": 1},
            sort=[("total_price", -1)],
            limit=limit
        )

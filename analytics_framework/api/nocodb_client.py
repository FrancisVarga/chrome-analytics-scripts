"""Client for interacting with the NocoDB API."""

import requests
import time
import logging
from typing import Dict, List, Optional, Any

from ..config import (
    NOCODB_BASE_URL,
    NOCODB_API_TOKEN,
    NOCODB_PROJECT_ID,
    MAX_RETRIES,
    RETRY_DELAY
)


class NocoDBClient:
    """Client for interacting with NocoDB API."""
    
    def __init__(
        self,
        base_url: str = NOCODB_BASE_URL,
        api_token: str = NOCODB_API_TOKEN,
        project_id: str = NOCODB_PROJECT_ID
    ):
        """
        Initialize the NocoDB client.
        
        Args:
            base_url: Base URL for the NocoDB API
            api_token: API token for authentication
            project_id: NocoDB project ID
        """
        self.base_url = base_url
        self.headers = {
            "xc-auth": api_token,
            "Content-Type": "application/json"
        }
        self.project_id = project_id
        self.logger = logging.getLogger(__name__)
    
    def _build_url(self, table_name: str) -> str:
        """
        Build the API URL for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            API URL
        """
        return f"{self.base_url}/api/v1/db/data/v2/noco/{self.project_id}/{table_name}"
    
    def fetch_records(
        self,
        table_name: str,
        where: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 1000,
        page: int = 1
    ) -> Dict[str, Any]:
        """
        Fetch records from a table with pagination.
        
        Args:
            table_name: Name of the table
            where: WHERE clause for filtering
            sort: Sorting criteria
            limit: Number of records per page (max 1000)
            page: Page number
            
        Returns:
            API response with records and pagination info
        """
        url = self._build_url(table_name)
        params = {"limit": limit, "page": page}
        
        if where:
            params["where"] = where
        
        if sort:
            params["sort"] = sort
        
        self.logger.debug(f"Fetching {table_name} with params: {params}")
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
                if attempt < MAX_RETRIES - 1:  # Don't sleep on the last attempt
                    time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                else:
                    raise
    
    def fetch_all_records(
        self,
        table_name: str,
        where: Optional[str] = None,
        sort: Optional[str] = None,
        batch_callback=None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all records from a table with automatic pagination.
        
        Args:
            table_name: Name of the table
            where: WHERE clause for filtering
            sort: Sorting criteria
            batch_callback: Optional callback function to process each batch
            
        Returns:
            List of all records
        """
        all_records = []
        page = 1
        has_more = True
        
        while has_more:
            self.logger.info(f"Fetching page {page} from {table_name}")
            
            try:
                response = self.fetch_records(
                    table_name=table_name,
                    where=where,
                    sort=sort,
                    page=page
                )
                
                if not response.get("list") or not isinstance(response["list"], list):
                    self.logger.warning(f"Unexpected API response format: {response}")
                    break
                
                records = response["list"]
                
                # Process batch if callback provided
                if batch_callback and callable(batch_callback):
                    batch_callback(records)
                else:
                    all_records.extend(records)
                
                # Check if there are more pages
                page_info = response.get("pageInfo", {})
                has_more = page_info.get("hasNextPage", False)
                page += 1
                
                # Add a small delay to avoid rate limiting
                if has_more:
                    time.sleep(0.1)
                    
            except Exception as e:
                self.logger.error(f"Error fetching records: {str(e)}")
                # Retry with exponential backoff
                time.sleep(RETRY_DELAY * (2 ** (page - 1)))
                # After 3 retries, give up
                if page > 3:
                    raise
        
        self.logger.info(f"Retrieved {len(all_records) if not batch_callback else 'all'} records from {table_name}")
        return all_records
    
    def create_record(self, table_name: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new record in a table.
        
        Args:
            table_name: Name of the table
            record: Record data to create
            
        Returns:
            Created record
        """
        url = self._build_url(table_name)
        
        self.logger.debug(f"Creating record in {table_name}")
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(url, headers=self.headers, json=record)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
                if attempt < MAX_RETRIES - 1:  # Don't sleep on the last attempt
                    time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                else:
                    raise
    
    def update_record(
        self,
        table_name: str,
        record_id: str,
        updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update an existing record.
        
        Args:
            table_name: Name of the table
            record_id: ID of the record to update
            updates: Fields to update
            
        Returns:
            Updated record
        """
        url = f"{self._build_url(table_name)}/{record_id}"
        
        self.logger.debug(f"Updating record {record_id} in {table_name}")
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.patch(url, headers=self.headers, json=updates)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
                if attempt < MAX_RETRIES - 1:  # Don't sleep on the last attempt
                    time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                else:
                    raise
    
    def delete_record(self, table_name: str, record_id: str) -> Dict[str, Any]:
        """
        Delete a record.
        
        Args:
            table_name: Name of the table
            record_id: ID of the record to delete
            
        Returns:
            Deletion response
        """
        url = f"{self._build_url(table_name)}/{record_id}"
        
        self.logger.debug(f"Deleting record {record_id} from {table_name}")
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.delete(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
                if attempt < MAX_RETRIES - 1:  # Don't sleep on the last attempt
                    time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                else:
                    raise
    
    def get_conversation_with_messages(self, conversation_id: str) -> Dict[str, Any]:
        """
        Get a conversation with its messages.
        
        Args:
            conversation_id: Conversation ID
            
        Returns:
            Dictionary with conversation and messages
        """
        # Fetch conversation
        conversation = None
        try:
            response = self.fetch_records(
                table_name="Conversation",
                where=f"(id,eq,{conversation_id})"
            )
            
            if response.get("list") and len(response["list"]) > 0:
                conversation = response["list"][0]
        except Exception as e:
            self.logger.error(f"Error fetching conversation {conversation_id}: {str(e)}")
            raise
        
        if not conversation:
            self.logger.warning(f"Conversation {conversation_id} not found")
            return {"conversation": None, "messages": []}
        
        # Fetch messages
        messages = []
        try:
            messages = self.fetch_all_records(
                table_name="Messages",
                where=f"(conversation_id,eq,{conversation_id})",
                sort="created_at"
            )
        except Exception as e:
            self.logger.error(f"Error fetching messages for conversation {conversation_id}: {str(e)}")
            raise
        
        return {
            "conversation": conversation,
            "messages": messages
        }
    
    def get_user_conversations(
        self,
        user_id: str,
        limit: int = 100,
        page: int = 1
    ) -> Dict[str, Any]:
        """
        Get conversations for a specific user.
        
        Args:
            user_id: User ID
            limit: Number of records per page
            page: Page number
            
        Returns:
            API response with conversations and pagination info
        """
        return self.fetch_records(
            table_name="Conversation",
            where=f"(from_end_user_id,eq,{user_id})",
            sort="created_at,desc",
            limit=limit,
            page=page
        )

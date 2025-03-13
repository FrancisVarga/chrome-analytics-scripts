"""Client for interacting with the NocoDB API."""

import logging
from typing import Dict, List, Optional, Any, Callable
import time

from ..config import (
    NOCODB_BASE_URL,
    NOCODB_API_TOKEN,
    NOCODB_PROJECT_ID,
    MAX_RETRIES,
    RETRY_DELAY,
    IO_THREADS
)
from ..utils.http_client import APIClient


class NocoDBClient:
    """Client for interacting with NocoDB API."""
    
    def __init__(
        self,
        base_url: str = NOCODB_BASE_URL,
        api_token: str = NOCODB_API_TOKEN,
        project_id: str = NOCODB_PROJECT_ID,
        max_retries: int = MAX_RETRIES,
        retry_delay: int = RETRY_DELAY,
        max_workers: int = IO_THREADS
    ):
        """
        Initialize the NocoDB client.
        
        Args:
            base_url: Base URL for the NocoDB API
            api_token: API token for authentication
            project_id: NocoDB project ID
            max_retries: Maximum number of retries for failed requests
            retry_delay: Delay between retries in seconds
            max_workers: Maximum number of worker threads for parallel requests
        """
        self.base_url = base_url
        self.project_id = project_id
        self.logger = logging.getLogger(__name__)
        
        # Initialize API client
        self.api_client = APIClient(
            base_url=base_url,
            api_key=api_token,
            auth_header="xc-auth",
            auth_prefix="",  # No prefix for NocoDB
            max_retries=max_retries,
            retry_delay=retry_delay,
            max_workers=max_workers
        )
    
    def _get_endpoint(self, table_name: str, record_id: Optional[str] = None) -> str:
        """
        Build the API endpoint for a table or record.
        
        Args:
            table_name: Name of the table
            record_id: Optional ID of a specific record
            
        Returns:
            API endpoint
        """
        endpoint = f"/api/v1/db/data/v2/noco/{self.project_id}/{table_name}"
        if record_id:
            endpoint = f"{endpoint}/{record_id}"
        return endpoint
    
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
        endpoint = self._get_endpoint(table_name)
        params = {"limit": limit, "page": page}
        
        if where:
            params["where"] = where
        
        if sort:
            params["sort"] = sort
        
        self.logger.debug(f"Fetching {table_name} with params: {params}")
        
        response = self.api_client.http_client.get(endpoint, params=params)
        return response.json()
    
    def fetch_all_records(
        self,
        table_name: str,
        where: Optional[str] = None,
        sort: Optional[str] = None,
        batch_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
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
        endpoint = self._get_endpoint(table_name)
        
        self.logger.debug(f"Creating record in {table_name}")
        
        response = self.api_client.http_client.post(endpoint, json=record)
        return response.json()
    
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
        endpoint = self._get_endpoint(table_name, record_id)
        
        self.logger.debug(f"Updating record {record_id} in {table_name}")
        
        response = self.api_client.http_client.patch(endpoint, json=updates)
        return response.json()
    
    def delete_record(self, table_name: str, record_id: str) -> Dict[str, Any]:
        """
        Delete a record.
        
        Args:
            table_name: Name of the table
            record_id: ID of the record to delete
            
        Returns:
            Deletion response
        """
        endpoint = self._get_endpoint(table_name, record_id)
        
        self.logger.debug(f"Deleting record {record_id} from {table_name}")
        
        response = self.api_client.http_client.delete(endpoint)
        return response.json()
    
    def get_conversation_with_messages(self, conversation_id: str) -> Dict[str, Any]:
        """
        Get a conversation with its messages.
        
        Args:
            conversation_id: Conversation ID
            
        Returns:
            Dictionary with conversation and messages
        """
        # Fetch conversation and messages in parallel
        endpoints = [
            self._get_endpoint("Conversation"),
            self._get_endpoint("Messages")
        ]
        
        params_list = [
            {"where": f"(id,eq,{conversation_id})"},
            {"where": f"(conversation_id,eq,{conversation_id})", "sort": "created_at"}
        ]
        
        responses = self.api_client.http_client.parallel_get(endpoints, params_list)
        
        # Process conversation response
        conversation = None
        if not isinstance(responses[0], Exception):
            conversation_response = responses[0].json()
            if conversation_response.get("list") and len(conversation_response["list"]) > 0:
                conversation = conversation_response["list"][0]
        
        if not conversation:
            self.logger.warning(f"Conversation {conversation_id} not found")
            return {"conversation": None, "messages": []}
        
        # Process messages response
        messages = []
        if not isinstance(responses[1], Exception):
            messages_response = responses[1].json()
            if messages_response.get("list") and isinstance(messages_response["list"], list):
                messages = messages_response["list"]
                
                # If there are more pages, fetch them
                page_info = messages_response.get("pageInfo", {})
                has_more = page_info.get("hasNextPage", False)
                
                if has_more:
                    # Fall back to sequential fetching for remaining pages
                    page = 2
                    while has_more:
                        response = self.fetch_records(
                            table_name="Messages",
                            where=f"(conversation_id,eq,{conversation_id})",
                            sort="created_at",
                            page=page
                        )
                        
                        if response.get("list") and isinstance(response["list"], list):
                            messages.extend(response["list"])
                        
                        page_info = response.get("pageInfo", {})
                        has_more = page_info.get("hasNextPage", False)
                        page += 1
                        
                        # Add a small delay to avoid rate limiting
                        if has_more:
                            time.sleep(0.1)
        
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
    
    def get_multiple_conversations(
        self,
        conversation_ids: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Get multiple conversations in parallel.
        
        Args:
            conversation_ids: List of conversation IDs
            
        Returns:
            List of conversations
        """
        if not conversation_ids:
            return []
        
        # Prepare endpoints and parameters
        endpoint = self._get_endpoint("Conversation")
        params_list = [{"where": f"(id,eq,{conv_id})"} for conv_id in conversation_ids]
        
        # Make parallel requests
        responses = self.api_client.http_client.parallel_get(
            [endpoint] * len(conversation_ids),
            params_list
        )
        
        # Process responses
        conversations = []
        for i, response in enumerate(responses):
            if isinstance(response, Exception):
                self.logger.error(f"Error fetching conversation {conversation_ids[i]}: {str(response)}")
                conversations.append(None)
            else:
                response_data = response.json()
                if response_data.get("list") and len(response_data["list"]) > 0:
                    conversations.append(response_data["list"][0])
                else:
                    conversations.append(None)
        
        return conversations
    
    def close(self):
        """Close the API client."""
        self.api_client.close()
    
    def __enter__(self):
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.close()

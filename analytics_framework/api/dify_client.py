"""Client for interacting with the Dify Workflow API."""

import json
import logging
import time
from typing import Dict, List, Any, Optional, Generator, Union

import requests
from requests.exceptions import RequestException

from ..config import (
    DIFY_API_KEY,
    DIFY_BASE_URL,
    MAX_RETRIES,
    RETRY_DELAY
)


class DifyClient:
    """Client for interacting with Dify Workflow API."""
    
    def __init__(
        self,
        api_key: str = DIFY_API_KEY,
        base_url: str = DIFY_BASE_URL
    ):
        """
        Initialize the Dify client.
        
        Args:
            api_key: API key for authentication
            base_url: Base URL for the Dify API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.logger = logging.getLogger(__name__)
    
    def execute_workflow(
        self,
        inputs: Dict[str, Any],
        response_mode: str = "blocking",
        user_id: str = "system_analytics",
        files: Optional[List[Dict[str, Any]]] = None
    ) -> Union[Dict[str, Any], Generator[Dict[str, Any], None, None]]:
        """
        Execute a Dify workflow.
        
        Args:
            inputs: Workflow input parameters
            response_mode: Response mode (streaming or blocking)
            user_id: Unique user identifier
            files: Optional list of files to include
            
        Returns:
            Workflow execution response or generator for streaming responses
        """
        url = f"{self.base_url}/workflows/run"
        
        data = {
            "inputs": inputs,
            "response_mode": response_mode,
            "user": user_id
        }
        
        if files:
            data["files"] = files
            
        self.logger.debug(f"Executing Dify workflow with inputs: {inputs}")
        
        if response_mode == "blocking":
            return self._execute_blocking(url, data)
        else:
            return self._execute_streaming(url, data)
    
    def _execute_blocking(self, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute workflow in blocking mode.
        
        Args:
            url: API endpoint URL
            data: Request data
            
        Returns:
            API response
        """
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(url, headers=self.headers, json=data)
                response.raise_for_status()
                result = response.json()
                self.logger.info(f"Workflow execution successful: {result.get('workflow_run_id')}")
                return result
            except RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                else:
                    raise
    
    def _execute_streaming(self, url: str, data: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """
        Execute workflow in streaming mode.
        
        Args:
            url: API endpoint URL
            data: Request data
            
        Returns:
            Generator yielding streaming responses
        """
        try:
            response = requests.post(url, headers=self.headers, json=data, stream=True)
            response.raise_for_status()
            
            for line in response.iter_lines():
                if line:
                    line = line.decode('utf-8')
                    if line.startswith('data:'):
                        data = json.loads(line[5:])
                        yield data
        except RequestException as e:
            self.logger.error(f"Streaming request failed: {str(e)}")
            raise
    
    def upload_file(self, file_path: str, file_type: str = "document") -> Dict[str, Any]:
        """
        Upload a file to be used in workflows.
        
        Args:
            file_path: Path to the file
            file_type: Type of file (document, image, audio, video, custom)
            
        Returns:
            API response with file ID
        """
        url = f"{self.base_url}/files/upload"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}"
            # Don't set Content-Type here, let requests set it for multipart/form-data
        }
        
        with open(file_path, 'rb') as file:
            files = {
                'file': (file_path.split('/')[-1], file, self._get_mime_type(file_path)),
            }
            
            data = {'type': file_type}
            
            for attempt in range(MAX_RETRIES):
                try:
                    response = requests.post(url, headers=headers, data=data, files=files)
                    response.raise_for_status()
                    result = response.json()
                    self.logger.info(f"File upload successful: {result.get('id')}")
                    return result
                except RequestException as e:
                    self.logger.warning(f"Upload failed (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (2 ** attempt))
                    else:
                        raise
    
    def get_workflow_result(self, workflow_run_id: str) -> Dict[str, Any]:
        """
        Get the result of a workflow execution.
        
        Args:
            workflow_run_id: ID of the workflow execution
            
        Returns:
            Workflow execution result
        """
        url = f"{self.base_url}/workflows/runs/{workflow_run_id}"
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (2 ** attempt))
                else:
                    raise
    
    def _get_mime_type(self, file_path: str) -> str:
        """
        Get the MIME type for a file based on its extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            MIME type string
        """
        ext = file_path.lower().split('.')[-1]
        
        # Common MIME types
        mime_types = {
            # Documents
            'txt': 'text/plain',
            'md': 'text/markdown',
            'pdf': 'application/pdf',
            'html': 'text/html',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'xls': 'application/vnd.ms-excel',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'csv': 'text/csv',
            
            # Images
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'webp': 'image/webp',
            'svg': 'image/svg+xml',
            
            # Audio
            'mp3': 'audio/mpeg',
            'm4a': 'audio/m4a',
            'wav': 'audio/wav',
            'webm': 'audio/webm',
            
            # Video
            'mp4': 'video/mp4',
            'mov': 'video/quicktime',
            'mpeg': 'video/mpeg',
        }
        
        return mime_types.get(ext, 'application/octet-stream')
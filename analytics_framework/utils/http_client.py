"""HTTP client library for making parallel requests."""

import logging
import time
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..config import (
    ENABLE_MULTITHREADING,
    IO_THREADS,
    MAX_RETRIES,
    RETRY_DELAY
)


class HTTPClient:
    """HTTP client for making requests with retry and parallel capabilities."""
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        max_retries: int = MAX_RETRIES,
        retry_delay: int = RETRY_DELAY,
        max_workers: int = IO_THREADS
    ):
        """
        Initialize the HTTP client.
        
        Args:
            base_url: Base URL for all requests
            headers: Default headers for all requests
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            retry_delay: Delay between retries in seconds
            max_workers: Maximum number of worker threads for parallel requests
        """
        self.logger = logging.getLogger(__name__)
        self.base_url = base_url
        self.headers = headers or {}
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.max_workers = max_workers
        
        # Create a session with retry configuration
        self.session = self._create_session()
        
        self.logger.debug(
            f"Initialized HTTP client with base_url={base_url}, "
            f"max_retries={max_retries}, retry_delay={retry_delay}, "
            f"max_workers={max_workers}"
        )
    
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry configuration.
        
        Returns:
            Configured requests session
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH"]
        )
        
        # Mount the retry adapter to the session
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update(self.headers)
        
        return session
    
    def _build_url(self, endpoint: str) -> str:
        """
        Build the full URL for a request.
        
        Args:
            endpoint: API endpoint
            
        Returns:
            Full URL
        """
        if self.base_url:
            # Handle cases where endpoint already starts with http(s)://
            if endpoint.startswith(("http://", "https://")):
                return endpoint
            
            # Handle cases where base_url doesn't end with / and endpoint doesn't start with /
            if not self.base_url.endswith("/") and not endpoint.startswith("/"):
                return f"{self.base_url}/{endpoint}"
            
            # Handle cases where base_url ends with / and endpoint starts with /
            if self.base_url.endswith("/") and endpoint.startswith("/"):
                return f"{self.base_url}{endpoint[1:]}"
            
            return f"{self.base_url}{endpoint}"
        
        return endpoint
    
    def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        retry_count: int = 0,
        **kwargs
    ) -> requests.Response:
        """
        Make an HTTP request.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            json: Request body JSON
            headers: Request headers
            timeout: Request timeout in seconds
            retry_count: Current retry count (used internally)
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        url = self._build_url(endpoint)
        timeout = timeout or self.timeout
        request_headers = {**self.headers, **(headers or {})}
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                json=json,
                headers=request_headers,
                timeout=timeout,
                **kwargs
            )
            
            # Log request details
            self.logger.debug(
                f"{method} {url} - Status: {response.status_code} - "
                f"Size: {len(response.content)} bytes"
            )
            
            # Raise an exception for 4xx/5xx status codes
            response.raise_for_status()
            
            return response
        
        except requests.exceptions.RequestException as e:
            # Log the error
            self.logger.error(f"Request error: {str(e)}")
            
            # Retry if we haven't exceeded max_retries
            if retry_count < self.max_retries:
                retry_count += 1
                wait_time = self.retry_delay * (2 ** (retry_count - 1))  # Exponential backoff
                
                self.logger.info(
                    f"Retrying {method} {url} in {wait_time} seconds "
                    f"(attempt {retry_count}/{self.max_retries})"
                )
                
                time.sleep(wait_time)
                
                return self.request(
                    method=method,
                    endpoint=endpoint,
                    params=params,
                    data=data,
                    json=json,
                    headers=headers,
                    timeout=timeout,
                    retry_count=retry_count,
                    **kwargs
                )
            
            # If we've exhausted retries, re-raise the exception
            raise
    
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make a GET request.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            headers: Request headers
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        return self.request(
            method="GET",
            endpoint=endpoint,
            params=params,
            headers=headers,
            **kwargs
        )
    
    def post(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make a POST request.
        
        Args:
            endpoint: API endpoint
            data: Request body data
            json: Request body JSON
            params: Query parameters
            headers: Request headers
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        return self.request(
            method="POST",
            endpoint=endpoint,
            data=data,
            json=json,
            params=params,
            headers=headers,
            **kwargs
        )
    
    def put(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make a PUT request.
        
        Args:
            endpoint: API endpoint
            data: Request body data
            json: Request body JSON
            params: Query parameters
            headers: Request headers
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        return self.request(
            method="PUT",
            endpoint=endpoint,
            data=data,
            json=json,
            params=params,
            headers=headers,
            **kwargs
        )
    
    def delete(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make a DELETE request.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            headers: Request headers
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        return self.request(
            method="DELETE",
            endpoint=endpoint,
            params=params,
            headers=headers,
            **kwargs
        )
    
    def patch(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make a PATCH request.
        
        Args:
            endpoint: API endpoint
            data: Request body data
            json: Request body JSON
            params: Query parameters
            headers: Request headers
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        return self.request(
            method="PATCH",
            endpoint=endpoint,
            data=data,
            json=json,
            params=params,
            headers=headers,
            **kwargs
        )
    
    def parallel_requests(
        self,
        requests_data: List[Dict[str, Any]],
        process_response: Optional[Callable[[requests.Response], Any]] = None
    ) -> List[Union[requests.Response, Any, Exception]]:
        """
        Make multiple HTTP requests in parallel.
        
        Args:
            requests_data: List of request configurations, each containing:
                - method: HTTP method (GET, POST, PUT, DELETE, etc.)
                - endpoint: API endpoint
                - params: Query parameters (optional)
                - data: Request body data (optional)
                - json: Request body JSON (optional)
                - headers: Request headers (optional)
                - timeout: Request timeout in seconds (optional)
                - **kwargs: Additional arguments for requests
            process_response: Function to process each response (optional)
            
        Returns:
            List of responses or processed results, with exceptions for failed requests
        """
        if not ENABLE_MULTITHREADING or len(requests_data) <= 1:
            # If multithreading is disabled or there's only one request, process sequentially
            results = []
            for req_data in requests_data:
                try:
                    method = req_data.pop("method")
                    endpoint = req_data.pop("endpoint")
                    
                    response = self.request(method=method, endpoint=endpoint, **req_data)
                    
                    if process_response:
                        results.append(process_response(response))
                    else:
                        results.append(response)
                except Exception as e:
                    self.logger.error(f"Error in request: {str(e)}")
                    results.append(e)
            
            return results
        
        # Process requests in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all requests to the executor
            futures = []
            for req_data in requests_data:
                method = req_data.pop("method")
                endpoint = req_data.pop("endpoint")
                
                future = executor.submit(
                    self.request,
                    method=method,
                    endpoint=endpoint,
                    **req_data
                )
                futures.append(future)
            
            # Collect results as they complete
            results = []
            for future in as_completed(futures):
                try:
                    response = future.result()
                    
                    if process_response:
                        results.append(process_response(response))
                    else:
                        results.append(response)
                except Exception as e:
                    self.logger.error(f"Error in parallel request: {str(e)}")
                    results.append(e)
            
            return results
    
    def parallel_get(
        self,
        endpoints: List[str],
        params_list: Optional[List[Optional[Dict[str, Any]]]] = None,
        headers_list: Optional[List[Optional[Dict[str, str]]]] = None,
        process_response: Optional[Callable[[requests.Response], Any]] = None
    ) -> List[Union[requests.Response, Any, Exception]]:
        """
        Make multiple GET requests in parallel.
        
        Args:
            endpoints: List of API endpoints
            params_list: List of query parameters for each endpoint (optional)
            headers_list: List of request headers for each endpoint (optional)
            process_response: Function to process each response (optional)
            
        Returns:
            List of responses or processed results, with exceptions for failed requests
        """
        # Prepare request data
        requests_data = []
        for i, endpoint in enumerate(endpoints):
            req_data = {
                "method": "GET",
                "endpoint": endpoint
            }
            
            # Add params if provided
            if params_list and i < len(params_list) and params_list[i] is not None:
                req_data["params"] = params_list[i]
            
            # Add headers if provided
            if headers_list and i < len(headers_list) and headers_list[i] is not None:
                req_data["headers"] = headers_list[i]
            
            requests_data.append(req_data)
        
        return self.parallel_requests(requests_data, process_response)
    
    def parallel_post(
        self,
        endpoints: List[str],
        data_list: Optional[List[Optional[Any]]] = None,
        json_list: Optional[List[Optional[Dict[str, Any]]]] = None,
        params_list: Optional[List[Optional[Dict[str, Any]]]] = None,
        headers_list: Optional[List[Optional[Dict[str, str]]]] = None,
        process_response: Optional[Callable[[requests.Response], Any]] = None
    ) -> List[Union[requests.Response, Any, Exception]]:
        """
        Make multiple POST requests in parallel.
        
        Args:
            endpoints: List of API endpoints
            data_list: List of request body data for each endpoint (optional)
            json_list: List of request body JSON for each endpoint (optional)
            params_list: List of query parameters for each endpoint (optional)
            headers_list: List of request headers for each endpoint (optional)
            process_response: Function to process each response (optional)
            
        Returns:
            List of responses or processed results, with exceptions for failed requests
        """
        # Prepare request data
        requests_data = []
        for i, endpoint in enumerate(endpoints):
            req_data = {
                "method": "POST",
                "endpoint": endpoint
            }
            
            # Add data if provided
            if data_list and i < len(data_list) and data_list[i] is not None:
                req_data["data"] = data_list[i]
            
            # Add json if provided
            if json_list and i < len(json_list) and json_list[i] is not None:
                req_data["json"] = json_list[i]
            
            # Add params if provided
            if params_list and i < len(params_list) and params_list[i] is not None:
                req_data["params"] = params_list[i]
            
            # Add headers if provided
            if headers_list and i < len(headers_list) and headers_list[i] is not None:
                req_data["headers"] = headers_list[i]
            
            requests_data.append(req_data)
        
        return self.parallel_requests(requests_data, process_response)
    
    def batch_requests(
        self,
        requests_data: List[Dict[str, Any]],
        batch_size: int = 10,
        process_response: Optional[Callable[[requests.Response], Any]] = None,
        batch_delay: float = 0
    ) -> List[Union[requests.Response, Any, Exception]]:
        """
        Make multiple HTTP requests in batches.
        
        Args:
            requests_data: List of request configurations
            batch_size: Number of requests to process in each batch
            process_response: Function to process each response (optional)
            batch_delay: Delay between batches in seconds (optional)
            
        Returns:
            List of responses or processed results, with exceptions for failed requests
        """
        results = []
        
        # Process requests in batches
        for i in range(0, len(requests_data), batch_size):
            batch = requests_data[i:i + batch_size]
            
            # Log batch processing
            self.logger.debug(
                f"Processing batch {i // batch_size + 1}/{(len(requests_data) + batch_size - 1) // batch_size} "
                f"({len(batch)} requests)"
            )
            
            # Process the batch
            batch_results = self.parallel_requests(batch, process_response)
            results.extend(batch_results)
            
            # Add delay between batches if specified
            if batch_delay > 0 and i + batch_size < len(requests_data):
                time.sleep(batch_delay)
        
        return results
    
    def close(self):
        """Close the session."""
        self.session.close()
        self.logger.debug("HTTP client session closed")
    
    def __enter__(self):
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.close()


class APIClient:
    """Base API client for interacting with REST APIs."""
    
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        auth_header: str = "Authorization",
        auth_prefix: str = "Bearer",
        timeout: int = 30,
        max_retries: int = MAX_RETRIES,
        retry_delay: int = RETRY_DELAY,
        max_workers: int = IO_THREADS
    ):
        """
        Initialize the API client.
        
        Args:
            base_url: Base URL for the API
            api_key: API key for authentication
            auth_header: Header name for authentication
            auth_prefix: Prefix for the authentication value
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            retry_delay: Delay between retries in seconds
            max_workers: Maximum number of worker threads for parallel requests
        """
        self.logger = logging.getLogger(__name__)
        self.base_url = base_url
        self.api_key = api_key
        self.auth_header = auth_header
        self.auth_prefix = auth_prefix
        
        # Prepare headers
        headers = {}
        if api_key:
            headers[auth_header] = f"{auth_prefix} {api_key}" if auth_prefix else api_key
        
        # Create HTTP client
        self.http_client = HTTPClient(
            base_url=base_url,
            headers=headers,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            max_workers=max_workers
        )
        
        self.logger.debug(f"Initialized API client for {base_url}")
    
    def close(self):
        """Close the HTTP client."""
        self.http_client.close()
    
    def __enter__(self):
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.close()


# Example usage:
"""
# Basic HTTP client usage
client = HTTPClient(base_url="https://api.example.com")
response = client.get("/users")
users = response.json()

# Parallel requests
responses = client.parallel_get(["/users", "/posts", "/comments"])
for response in responses:
    if isinstance(response, Exception):
        print(f"Request failed: {response}")
    else:
        print(f"Status code: {response.status_code}")
        print(f"Data: {response.json()}")

# API client with authentication
api_client = APIClient(
    base_url="https://api.example.com",
    api_key="your-api-key"
)
response = api_client.http_client.get("/users")
users = response.json()

# Process responses in parallel
def extract_user_names(response):
    users = response.json()
    return [user["name"] for user in users]

user_names = api_client.http_client.parallel_get(
    ["/users/1", "/users/2", "/users/3"],
    process_response=extract_user_names
)
"""

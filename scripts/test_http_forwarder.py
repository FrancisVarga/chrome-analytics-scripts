#!/usr/bin/env python
"""Test script for the HTTP forwarder."""

import argparse
import json
import logging
import requests
import sys
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_http_forwarder(
    forwarder_url: str,
    target_url: str,
    method: str = 'GET',
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Test the HTTP forwarder by sending a request to it.
    
    Args:
        forwarder_url: URL of the HTTP forwarder
        target_url: URL to forward the request to
        method: HTTP method to use
        headers: Headers to include in the forwarded request
        data: Data to include in the forwarded request
        
    Returns:
        Response from the HTTP forwarder
    """
    # Prepare the payload
    payload = {
        "fwd_data": {
            "url": target_url,
            "method": method,
            "headers": headers or {},
            "data": data or {}
        }
    }
    
    logger.info(f"Sending request to HTTP forwarder at {forwarder_url}")
    logger.info(f"Forwarding to {target_url} with method {method}")
    
    # Send the request to the HTTP forwarder
    response = requests.post(forwarder_url, json=payload)
    
    # Check if the request was successful
    if response.status_code == 200:
        logger.info("Request forwarded successfully")
        return response.json()
    else:
        logger.error(f"Error forwarding request: {response.text}")
        return {"error": response.text}

def main():
    """Run the HTTP forwarder test."""
    parser = argparse.ArgumentParser(description='HTTP Forwarder Test')
    parser.add_argument('--forwarder-url', default='http://localhost:5000/forward', 
                        help='URL of the HTTP forwarder')
    parser.add_argument('--target-url', default='https://httpbin.org/anything', 
                        help='URL to forward the request to')
    parser.add_argument('--method', default='POST', 
                        help='HTTP method to use')
    parser.add_argument('--headers', default='{}', 
                        help='Headers to include in the forwarded request (JSON string)')
    parser.add_argument('--data', default='{"test": "data"}', 
                        help='Data to include in the forwarded request (JSON string)')
    
    args = parser.parse_args()
    
    # Parse JSON strings
    try:
        headers = json.loads(args.headers)
        data = json.loads(args.data)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {str(e)}")
        sys.exit(1)
    
    # Test the HTTP forwarder
    result = test_http_forwarder(
        forwarder_url=args.forwarder_url,
        target_url=args.target_url,
        method=args.method,
        headers=headers,
        data=data
    )
    
    # Print the result
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()

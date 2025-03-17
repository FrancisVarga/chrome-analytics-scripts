#!/usr/bin/env python
"""HTTP forwarder script that receives a POST JSON with fwd_data object and forwards the request."""

import argparse
import logging
import requests
import sys
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify

sys.path.append('.')  # Add the current directory to the path
from analytics_framework.utils.http_client import HTTPClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/forward', methods=['POST'])
def forward_request():
    """
    Forward an HTTP request based on the fwd_data object.
    
    Expected JSON structure:
    {
        "fwd_data": {
            "url": "https://example.com/api/endpoint",
            "method": "POST",
            "headers": {"Content-Type": "application/json", "Authorization": "Bearer token"},
            "data": {"key": "value"}
        }
    }
    
    Returns:
        JSON response from the forwarded request
    """
    try:
        # Get the request JSON
        request_json = request.get_json()
        
        if not request_json:
            return jsonify({"error": "No JSON data provided"}), 400
        
        # Extract the fwd_data object
        fwd_data = request_json.get('fwd_data')
        
        if not fwd_data:
            return jsonify({"error": "Missing fwd_data object"}), 400
        
        # Extract required fields
        url = fwd_data.get('url')
        method = fwd_data.get('method', 'GET')
        headers = fwd_data.get('headers', {})
        data = fwd_data.get('data')
        
        if not url:
            return jsonify({"error": "Missing url in fwd_data"}), 400
        
        logger.info(f"Forwarding request to {url} with method {method}")
        
        # Create HTTP client
        http_client = HTTPClient()
        
        # Forward the request
        response = forward_http_request(
            http_client=http_client,
            url=url,
            method=method,
            headers=headers,
            data=data
        )
        
        # Close the HTTP client
        http_client.close()
        
        # Return the response
        return jsonify({
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "content": response.text,
            "content_type": response.headers.get('Content-Type')
        })
    
    except Exception as e:
        logger.error(f"Error forwarding request: {str(e)}")
        return jsonify({"error": str(e)}), 500

def forward_http_request(
    http_client: HTTPClient,
    url: str,
    method: str = 'GET',
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Any] = None
) -> requests.Response:
    """
    Forward an HTTP request using the HTTP client.
    
    Args:
        http_client: HTTP client instance
        url: Target URL
        method: HTTP method (GET, POST, PUT, DELETE, etc.)
        headers: Request headers
        data: Request data (will be sent as JSON for POST/PUT/PATCH)
        
    Returns:
        Response from the forwarded request
    """
    method = method.upper()
    
    if method == 'GET':
        return http_client.get(url, headers=headers, params=data)
    elif method == 'POST':
        return http_client.post(url, headers=headers, json=data)
    elif method == 'PUT':
        return http_client.put(url, headers=headers, json=data)
    elif method == 'DELETE':
        return http_client.delete(url, headers=headers, params=data)
    elif method == 'PATCH':
        return http_client.patch(url, headers=headers, json=data)
    else:
        # For other methods, use the generic request method
        return http_client.request(method, url, headers=headers, json=data)

def main():
    """Run the HTTP forwarder server."""
    parser = argparse.ArgumentParser(description='HTTP Forwarder Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind the server to')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    
    args = parser.parse_args()
    
    logger.info(f"Starting HTTP forwarder server on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == '__main__':
    main()

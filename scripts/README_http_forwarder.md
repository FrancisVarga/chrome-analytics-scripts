# HTTP Forwarder Script

This script provides a simple HTTP forwarding service that receives a POST request with a `fwd_data` object and forwards it to the specified URL.

## Features

- Forwards HTTP requests to specified URLs
- Supports all common HTTP methods (GET, POST, PUT, DELETE, PATCH)
- Handles headers and request data
- Returns the response from the forwarded request
- Uses the project's HTTP client with retry capabilities

## Installation

The HTTP forwarder script requires Flask, which is not included in the project's default dependencies. Install it using:

```bash
pip install flask
```

## Usage

### Starting the Server

Run the HTTP forwarder server:

```bash
python scripts/http_forwarder.py --host 0.0.0.0 --port 5000
```

Options:

- `--host`: Host to bind the server to (default: 0.0.0.0)
- `--port`: Port to bind the server to (default: 5000)
- `--debug`: Run in debug mode (optional)

### Making Requests

Send a POST request to the `/forward` endpoint with a JSON body containing a `fwd_data` object:

```json
{
  "fwd_data": {
    "url": "https://example.com/api/endpoint",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "Authorization": "Bearer token"
    },
    "data": {
      "key": "value"
    }
  }
}
```

#### Required Fields

- `url`: The target URL to forward the request to

#### Optional Fields

- `method`: HTTP method to use (default: "GET")
- `headers`: Headers to include in the forwarded request
- `data`: Data to include in the forwarded request

### Example Using cURL

```bash
curl -X POST http://localhost:5000/forward \
  -H "Content-Type: application/json" \
  -d '{
    "fwd_data": {
      "url": "https://httpbin.org/post",
      "method": "POST",
      "headers": {
        "Content-Type": "application/json",
        "X-Custom-Header": "Custom Value"
      },
      "data": {
        "name": "John Doe",
        "email": "john@example.com"
      }
    }
  }'
```

### Example Using Python Requests

```python
import requests

url = "http://localhost:5000/forward"
payload = {
    "fwd_data": {
        "url": "https://httpbin.org/post",
        "method": "POST",
        "headers": {
            "Content-Type": "application/json",
            "X-Custom-Header": "Custom Value"
        },
        "data": {
            "name": "John Doe",
            "email": "john@example.com"
        }
    }
}

response = requests.post(url, json=payload)
print(response.json())
```

## Response Format

The HTTP forwarder returns a JSON response with the following structure:

```json
{
  "status_code": 200,
  "headers": {
    "Content-Type": "application/json",
    "Date": "Mon, 17 Mar 2025 10:52:30 GMT",
    ...
  },
  "content": "Response content as string",
  "content_type": "application/json"
}
```

## Error Handling

If an error occurs during the forwarding process, the server returns a JSON response with an error message and an appropriate HTTP status code:

```json
{
  "error": "Error message"
}
```

Common error codes:

- 400: Bad Request (missing required fields)
- 500: Internal Server Error (error during forwarding)

## Integration with the Project

The HTTP forwarder script uses the project's HTTP client library (`analytics_framework.utils.http_client.HTTPClient`) to make the forwarded requests. This ensures that all requests benefit from the same retry logic, timeout handling, and other features provided by the HTTP client.

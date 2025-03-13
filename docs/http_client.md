# HTTP Client Library

The HTTP client library provides a robust and flexible way to make HTTP requests, with support for parallel requests, retries, and response processing. It is designed to be simple to use while providing powerful features for handling complex API interactions.

## Features

- **Simple Request Methods**: Easy-to-use methods for common HTTP operations (GET, POST, PUT, DELETE, PATCH)
- **Parallel Requests**: Make multiple requests in parallel to improve performance
- **Automatic Retries**: Built-in retry mechanism with exponential backoff for transient failures
- **Batch Processing**: Process large numbers of requests in configurable batches
- **Response Processing**: Transform responses with custom processing functions
- **Session Management**: Efficient connection pooling and session reuse
- **Context Manager Support**: Clean resource management with context manager pattern
- **Comprehensive Logging**: Detailed logging for debugging and monitoring
- **Configurable Timeouts**: Control request timeouts to prevent hanging operations
- **Authentication Support**: Easy API key and token-based authentication

## Classes

### HTTPClient

The `HTTPClient` class is the core of the library, providing methods for making HTTP requests with retry capabilities and parallel execution.

```python
from analytics_framework.utils.http_client import HTTPClient

# Create a client
client = HTTPClient(
    base_url="https://api.example.com",
    headers={"User-Agent": "MyApp/1.0"},
    timeout=30,
    max_retries=3,
    retry_delay=2,
    max_workers=4
)

# Make a simple GET request
response = client.get("/users")
users = response.json()

# Make a POST request with JSON data
new_user = {"name": "John Doe", "email": "john@example.com"}
response = client.post("/users", json=new_user)
user = response.json()

# Close the client when done
client.close()

# Or use as a context manager
with HTTPClient(base_url="https://api.example.com") as client:
    response = client.get("/users")
    users = response.json()
```

### APIClient

The `APIClient` class is a higher-level wrapper around `HTTPClient` that adds API-specific functionality like authentication.

```python
from analytics_framework.utils.http_client import APIClient

# Create an API client with authentication
api_client = APIClient(
    base_url="https://api.example.com",
    api_key="your-api-key",
    auth_header="Authorization",
    auth_prefix="Bearer",
    timeout=30,
    max_retries=3,
    retry_delay=2,
    max_workers=4
)

# Make requests using the http_client property
response = api_client.http_client.get("/users")
users = response.json()

# Close the client when done
api_client.close()

# Or use as a context manager
with APIClient(base_url="https://api.example.com", api_key="your-api-key") as api_client:
    response = api_client.http_client.get("/users")
    users = response.json()
```

## Parallel Requests

The library provides several methods for making parallel requests:

### parallel_requests

The most flexible method for making parallel requests, allowing different HTTP methods and configurations for each request.

```python
requests_data = [
    {
        "method": "GET",
        "endpoint": "/users/1",
        "params": {"include": "profile"}
    },
    {
        "method": "POST",
        "endpoint": "/posts",
        "json": {"title": "New Post", "body": "Content"}
    },
    {
        "method": "PUT",
        "endpoint": "/users/2",
        "json": {"name": "Updated Name"}
    }
]

responses = client.parallel_requests(requests_data)
```

### parallel_get

A convenience method for making multiple GET requests in parallel.

```python
endpoints = ["/users/1", "/users/2", "/users/3"]
params_list = [{"include": "profile"}, {"include": "posts"}, None]
responses = client.parallel_get(endpoints, params_list)
```

### parallel_post

A convenience method for making multiple POST requests in parallel.

```python
endpoints = ["/posts", "/posts", "/posts"]
json_list = [
    {"title": "Post 1", "body": "Content 1"},
    {"title": "Post 2", "body": "Content 2"},
    {"title": "Post 3", "body": "Content 3"}
]
responses = client.parallel_post(endpoints, json_list=json_list)
```

## Response Processing

You can process responses in parallel by providing a `process_response` function:

```python
def extract_user_data(response):
    user = response.json()
    return {
        "id": user["id"],
        "name": user["name"],
        "email": user["email"]
    }

user_data = client.parallel_get(
    ["/users/1", "/users/2", "/users/3"],
    process_response=extract_user_data
)
```

## Batch Processing

For large numbers of requests, you can process them in batches to avoid overwhelming the server:

```python
requests_data = [
    {"method": "GET", "endpoint": f"/posts/{i}"}
    for i in range(1, 101)
]

responses = client.batch_requests(
    requests_data,
    batch_size=10,  # Process 10 requests at a time
    batch_delay=1.0,  # Wait 1 second between batches
    process_response=lambda r: r.json()["title"]
)
```

## Error Handling

The library handles errors gracefully, returning exceptions in the results list for failed requests:

```python
responses = client.parallel_get(["/valid", "/invalid", "/valid2"])

for response in responses:
    if isinstance(response, Exception):
        print(f"Request failed: {response}")
    else:
        print(f"Success: {response.status_code}")
```

## Configuration

The HTTP client library uses the following configuration settings from `analytics_framework.config`:

- `ENABLE_MULTITHREADING`: Whether to enable parallel processing (default: true)
- `IO_THREADS`: Number of I/O threads for parallel requests (default: 4)
- `MAX_RETRIES`: Maximum number of retries for failed requests (default: 3)
- `RETRY_DELAY`: Delay between retries in seconds (default: 2)

You can override these settings when creating a client:

```python
client = HTTPClient(
    max_retries=5,
    retry_delay=1,
    max_workers=8
)
```

## Example Usage

See the [example script](../examples/http_client_example.py) for complete usage examples.

```python
# Run the example script
python -m examples.http_client_example

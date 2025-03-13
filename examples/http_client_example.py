"""Example script demonstrating the use of the HTTP client library."""

import logging
import json
from analytics_framework.utils.http_client import HTTPClient, APIClient
from analytics_framework.config import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)


def basic_request_example():
    """Demonstrate basic HTTP requests."""
    logger.info("Running basic request example")
    
    # Create an HTTP client
    client = HTTPClient(base_url="https://jsonplaceholder.typicode.com")
    
    # Make a GET request
    response = client.get("/users/1")
    user = response.json()
    logger.info(f"User: {user['name']} (ID: {user['id']})")
    
    # Make a POST request
    new_post = {
        "title": "Test Post",
        "body": "This is a test post",
        "userId": 1
    }
    response = client.post("/posts", json=new_post)
    post = response.json()
    logger.info(f"Created post with ID: {post['id']}")
    
    # Close the client
    client.close()


def parallel_requests_example():
    """Demonstrate parallel HTTP requests."""
    logger.info("Running parallel requests example")
    
    # Create an HTTP client
    client = HTTPClient(base_url="https://jsonplaceholder.typicode.com")
    
    # Make parallel GET requests
    endpoints = [f"/users/{i}" for i in range(1, 6)]
    responses = client.parallel_get(endpoints)
    
    # Process the responses
    for i, response in enumerate(responses):
        if isinstance(response, Exception):
            logger.error(f"Request {i+1} failed: {response}")
        else:
            user = response.json()
            logger.info(f"User {i+1}: {user['name']} (ID: {user['id']})")
    
    # Define a response processor function
    def extract_user_email(response):
        user = response.json()
        return {
            "id": user["id"],
            "name": user["name"],
            "email": user["email"]
        }
    
    # Make parallel GET requests with response processing
    user_data = client.parallel_get(
        endpoints,
        process_response=extract_user_email
    )
    
    logger.info(f"Processed user data: {json.dumps(user_data, indent=2)}")
    
    # Close the client
    client.close()


def batch_requests_example():
    """Demonstrate batch HTTP requests."""
    logger.info("Running batch requests example")
    
    # Create an HTTP client
    client = HTTPClient(base_url="https://jsonplaceholder.typicode.com")
    
    # Prepare request data for multiple posts
    requests_data = []
    for i in range(1, 11):
        requests_data.append({
            "method": "GET",
            "endpoint": f"/posts/{i}"
        })
    
    # Process requests in batches
    responses = client.batch_requests(
        requests_data,
        batch_size=3,
        batch_delay=0.5
    )
    
    # Process the responses
    for i, response in enumerate(responses):
        if isinstance(response, Exception):
            logger.error(f"Request {i+1} failed: {response}")
        else:
            post = response.json()
            logger.info(f"Post {i+1}: {post['title'][:30]}...")
    
    # Close the client
    client.close()


def api_client_example():
    """Demonstrate API client usage."""
    logger.info("Running API client example")
    
    # Create an API client with authentication
    api_client = APIClient(
        base_url="https://jsonplaceholder.typicode.com",
        api_key="fake-api-key",  # Not needed for this public API
        auth_header="X-API-Key"
    )
    
    # Make a GET request
    response = api_client.http_client.get("/users/1")
    user = response.json()
    logger.info(f"User: {user['name']} (ID: {user['id']})")
    
    # Make parallel POST requests
    new_posts = [
        {
            "title": f"Test Post {i}",
            "body": f"This is test post {i}",
            "userId": 1
        }
        for i in range(1, 4)
    ]
    
    responses = api_client.http_client.parallel_post(
        ["/posts"] * len(new_posts),
        json_list=new_posts
    )
    
    # Process the responses
    for i, response in enumerate(responses):
        if isinstance(response, Exception):
            logger.error(f"Request {i+1} failed: {response}")
        else:
            post = response.json()
            logger.info(f"Created post {i+1} with ID: {post['id']}")
    
    # Close the client
    api_client.close()


def context_manager_example():
    """Demonstrate context manager usage."""
    logger.info("Running context manager example")
    
    # Use HTTP client as a context manager
    with HTTPClient(base_url="https://jsonplaceholder.typicode.com") as client:
        response = client.get("/users/1")
        user = response.json()
        logger.info(f"User: {user['name']} (ID: {user['id']})")
    
    # Use API client as a context manager
    with APIClient(base_url="https://jsonplaceholder.typicode.com") as api_client:
        response = api_client.http_client.get("/users/2")
        user = response.json()
        logger.info(f"User: {user['name']} (ID: {user['id']})")


def main():
    """Run all examples."""
    logger.info("Starting HTTP client examples")
    
    try:
        basic_request_example()
        parallel_requests_example()
        batch_requests_example()
        api_client_example()
        context_manager_example()
        
        logger.info("All examples completed successfully")
    except Exception as e:
        logger.error(f"Error running examples: {str(e)}")


if __name__ == "__main__":
    main()

"""Test script to demonstrate HTTP client performance with parallel requests."""

import time
import logging
import statistics
from typing import List, Dict, Any
import argparse

from analytics_framework.utils.http_client import HTTPClient
from analytics_framework.config import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)


def make_sequential_requests(
    client: HTTPClient,
    endpoints: List[str],
    num_runs: int = 3
) -> Dict[str, Any]:
    """
    Make sequential HTTP requests and measure performance.
    
    Args:
        client: HTTP client
        endpoints: List of API endpoints
        num_runs: Number of test runs
        
    Returns:
        Performance metrics
    """
    logger.info(f"Making {len(endpoints)} sequential requests ({num_runs} runs)")
    
    run_times = []
    
    for run in range(num_runs):
        start_time = time.time()
        
        for endpoint in endpoints:
            try:
                client.get(endpoint)
            except Exception as e:
                logger.error(f"Request failed: {str(e)}")
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        run_times.append(elapsed_time)
        
        logger.info(f"Run {run+1}: {elapsed_time:.2f} seconds")
    
    return {
        "min": min(run_times),
        "max": max(run_times),
        "avg": statistics.mean(run_times),
        "median": statistics.median(run_times),
        "total_requests": len(endpoints) * num_runs
    }


def make_parallel_requests(
    client: HTTPClient,
    endpoints: List[str],
    num_runs: int = 3
) -> Dict[str, Any]:
    """
    Make parallel HTTP requests and measure performance.
    
    Args:
        client: HTTP client
        endpoints: List of API endpoints
        num_runs: Number of test runs
        
    Returns:
        Performance metrics
    """
    logger.info(f"Making {len(endpoints)} parallel requests ({num_runs} runs)")
    
    run_times = []
    
    for run in range(num_runs):
        start_time = time.time()
        
        responses = client.parallel_get(endpoints)
        
        # Count successful and failed requests
        success_count = sum(1 for r in responses if not isinstance(r, Exception))
        error_count = sum(1 for r in responses if isinstance(r, Exception))
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        run_times.append(elapsed_time)
        
        logger.info(
            f"Run {run+1}: {elapsed_time:.2f} seconds "
            f"({success_count} successful, {error_count} failed)"
        )
    
    return {
        "min": min(run_times),
        "max": max(run_times),
        "avg": statistics.mean(run_times),
        "median": statistics.median(run_times),
        "total_requests": len(endpoints) * num_runs
    }


def make_batch_requests(
    client: HTTPClient,
    endpoints: List[str],
    batch_size: int = 10,
    num_runs: int = 3
) -> Dict[str, Any]:
    """
    Make batch HTTP requests and measure performance.
    
    Args:
        client: HTTP client
        endpoints: List of API endpoints
        batch_size: Number of requests per batch
        num_runs: Number of test runs
        
    Returns:
        Performance metrics
    """
    logger.info(
        f"Making {len(endpoints)} batch requests with batch size {batch_size} "
        f"({num_runs} runs)"
    )
    
    run_times = []
    
    for run in range(num_runs):
        start_time = time.time()
        
        # Prepare request data
        requests_data = [
            {"method": "GET", "endpoint": endpoint}
            for endpoint in endpoints
        ]
        
        responses = client.batch_requests(requests_data, batch_size=batch_size)
        
        # Count successful and failed requests
        success_count = sum(1 for r in responses if not isinstance(r, Exception))
        error_count = sum(1 for r in responses if isinstance(r, Exception))
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        run_times.append(elapsed_time)
        
        logger.info(
            f"Run {run+1}: {elapsed_time:.2f} seconds "
            f"({success_count} successful, {error_count} failed)"
        )
    
    return {
        "min": min(run_times),
        "max": max(run_times),
        "avg": statistics.mean(run_times),
        "median": statistics.median(run_times),
        "total_requests": len(endpoints) * num_runs
    }


def print_performance_comparison(
    sequential_metrics: Dict[str, Any],
    parallel_metrics: Dict[str, Any],
    batch_metrics: Dict[str, Any]
) -> None:
    """
    Print performance comparison between sequential and parallel requests.
    
    Args:
        sequential_metrics: Sequential request metrics
        parallel_metrics: Parallel request metrics
        batch_metrics: Batch request metrics
    """
    # Calculate speedup
    sequential_avg = sequential_metrics["avg"]
    parallel_avg = parallel_metrics["avg"]
    batch_avg = batch_metrics["avg"]
    
    parallel_speedup = sequential_avg / parallel_avg if parallel_avg > 0 else 0
    batch_speedup = sequential_avg / batch_avg if batch_avg > 0 else 0
    
    # Print comparison
    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON")
    print("=" * 60)
    print(f"Total requests: {sequential_metrics['total_requests']}")
    print("\nSequential Requests:")
    print(f"  Min time:   {sequential_metrics['min']:.2f} seconds")
    print(f"  Max time:   {sequential_metrics['max']:.2f} seconds")
    print(f"  Avg time:   {sequential_metrics['avg']:.2f} seconds")
    print(f"  Median time: {sequential_metrics['median']:.2f} seconds")
    
    print("\nParallel Requests:")
    print(f"  Min time:   {parallel_metrics['min']:.2f} seconds")
    print(f"  Max time:   {parallel_metrics['max']:.2f} seconds")
    print(f"  Avg time:   {parallel_metrics['avg']:.2f} seconds")
    print(f"  Median time: {parallel_metrics['median']:.2f} seconds")
    print(f"  Speedup:    {parallel_speedup:.2f}x")
    
    print("\nBatch Requests:")
    print(f"  Min time:   {batch_metrics['min']:.2f} seconds")
    print(f"  Max time:   {batch_metrics['max']:.2f} seconds")
    print(f"  Avg time:   {batch_metrics['avg']:.2f} seconds")
    print(f"  Median time: {batch_metrics['median']:.2f} seconds")
    print(f"  Speedup:    {batch_speedup:.2f}x")
    print("=" * 60)


def main():
    """Run the performance test."""
    parser = argparse.ArgumentParser(description="HTTP client performance test")
    parser.add_argument(
        "--num-requests",
        type=int,
        default=50,
        help="Number of requests to make (default: 50)"
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=3,
        help="Number of test runs (default: 3)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for batch requests (default: 10)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of worker threads (default: 10)"
    )
    
    args = parser.parse_args()
    
    # Create a list of endpoints
    endpoints = [
        f"https://jsonplaceholder.typicode.com/posts/{i}"
        for i in range(1, args.num_requests + 1)
    ]
    
    # Create HTTP client
    client = HTTPClient(max_workers=args.max_workers)
    
    try:
        # Make sequential requests
        sequential_metrics = make_sequential_requests(
            client,
            endpoints,
            num_runs=args.num_runs
        )
        
        # Make parallel requests
        parallel_metrics = make_parallel_requests(
            client,
            endpoints,
            num_runs=args.num_runs
        )
        
        # Make batch requests
        batch_metrics = make_batch_requests(
            client,
            endpoints,
            batch_size=args.batch_size,
            num_runs=args.num_runs
        )
        
        # Print performance comparison
        print_performance_comparison(
            sequential_metrics,
            parallel_metrics,
            batch_metrics
        )
    
    finally:
        # Close the client
        client.close()


if __name__ == "__main__":
    main()

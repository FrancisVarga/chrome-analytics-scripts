"""Thread pool manager for multi-threaded processing."""

import logging
from typing import Callable, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import threading
from queue import Queue

from ..config import (
    ENABLE_MULTITHREADING,
    MAX_WORKERS,
    CHUNK_SIZE,
    IO_THREADS,
    PROCESSING_THREADS
)


class ThreadPoolManager:
    """Manager for thread pools and parallel processing."""
    
    def __init__(self):
        """Initialize the thread pool manager."""
        self.logger = logging.getLogger(__name__)
        self.io_executor = ThreadPoolExecutor(max_workers=IO_THREADS)
        self.processing_executor = ThreadPoolExecutor(max_workers=PROCESSING_THREADS)
        self.process_executor = ProcessPoolExecutor(max_workers=max(1, MAX_WORKERS // 2))
        
        # Thread-local storage
        self.thread_local = threading.local()
        
        # Shared resources
        self.locks = {}
        
        self.logger.info(
            f"Initialized thread pool manager with {IO_THREADS} IO threads, "
            f"{PROCESSING_THREADS} processing threads, and "
            f"{max(1, MAX_WORKERS // 2)} process workers"
        )
    
    def get_lock(self, resource_name: str) -> threading.Lock:
        """
        Get a lock for a specific resource.
        
        Args:
            resource_name: Name of the resource
            
        Returns:
            Lock for the resource
        """
        if resource_name not in self.locks:
            self.locks[resource_name] = threading.Lock()
        
        return self.locks[resource_name]
    
    def map_io(self, func: Callable, items: List[Any], *args, **kwargs) -> List[Any]:
        """
        Map a function over items using the IO thread pool.
        
        Args:
            func: Function to apply
            items: Items to process
            *args: Additional arguments for the function
            **kwargs: Additional keyword arguments for the function
            
        Returns:
            List of results
        """
        if not ENABLE_MULTITHREADING or len(items) <= 1:
            return [func(item, *args, **kwargs) for item in items]
        
        futures = []
        for item in items:
            future = self.io_executor.submit(func, item, *args, **kwargs)
            futures.append(future)
        
        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error in IO thread: {str(e)}")
                results.append(None)
        
        return results
    
    def map_processing(self, func: Callable, items: List[Any], *args, **kwargs) -> List[Any]:
        """
        Map a function over items using the processing thread pool.
        
        Args:
            func: Function to apply
            items: Items to process
            *args: Additional arguments for the function
            **kwargs: Additional keyword arguments for the function
            
        Returns:
            List of results
        """
        if not ENABLE_MULTITHREADING or len(items) <= 1:
            return [func(item, *args, **kwargs) for item in items]
        
        futures = []
        for item in items:
            future = self.processing_executor.submit(func, item, *args, **kwargs)
            futures.append(future)
        
        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error in processing thread: {str(e)}")
                results.append(None)
        
        return results
    
    def map_process(self, func: Callable, items: List[Any], *args, **kwargs) -> List[Any]:
        """
        Map a function over items using the process pool.
        
        Args:
            func: Function to apply
            items: Items to process
            *args: Additional arguments for the function
            **kwargs: Additional keyword arguments for the function
            
        Returns:
            List of results
        """
        if not ENABLE_MULTITHREADING or len(items) <= 1:
            return [func(item, *args, **kwargs) for item in items]
        
        # Split items into chunks for better performance
        chunks = [items[i:i + CHUNK_SIZE] for i in range(0, len(items), CHUNK_SIZE)]
        
        # Define a wrapper function that processes a chunk
        def process_chunk(chunk):
            return [func(item, *args, **kwargs) for item in chunk]
        
        futures = []
        for chunk in chunks:
            future = self.process_executor.submit(process_chunk, chunk)
            futures.append(future)
        
        results = []
        for future in as_completed(futures):
            try:
                chunk_results = future.result()
                results.extend(chunk_results)
            except Exception as e:
                self.logger.error(f"Error in process worker: {str(e)}")
        
        return results
    
    def parallel_for_each(
        self,
        func: Callable,
        items: List[Any],
        use_processes: bool = False,
        *args,
        **kwargs
    ) -> None:
        """
        Apply a function to each item in parallel.
        
        Args:
            func: Function to apply
            items: Items to process
            use_processes: Whether to use processes instead of threads
            *args: Additional arguments for the function
            **kwargs: Additional keyword arguments for the function
        """
        if not ENABLE_MULTITHREADING or len(items) <= 1:
            for item in items:
                func(item, *args, **kwargs)
            return
        
        if use_processes:
            self.map_process(func, items, *args, **kwargs)
        else:
            self.map_processing(func, items, *args, **kwargs)
    
    def shutdown(self):
        """Shutdown all thread pools."""
        self.io_executor.shutdown(wait=True)
        self.processing_executor.shutdown(wait=True)
        self.process_executor.shutdown(wait=True)
        self.logger.info("Thread pool manager shutdown complete")


# Global thread pool manager instance
thread_pool_manager = ThreadPoolManager()


class WorkQueue:
    """Thread-safe work queue for parallel processing."""
    
    def __init__(self, max_size: int = 1000):
        """
        Initialize the work queue.
        
        Args:
            max_size: Maximum queue size
        """
        self.queue = Queue(maxsize=max_size)
        self.logger = logging.getLogger(__name__)
    
    def put(self, item: Any) -> None:
        """
        Put an item in the queue.
        
        Args:
            item: Item to put in the queue
        """
        self.queue.put(item)
    
    def get(self) -> Any:
        """
        Get an item from the queue.
        
        Returns:
            Item from the queue
        """
        return self.queue.get()
    
    def task_done(self) -> None:
        """Mark a task as done."""
        self.queue.task_done()
    
    def join(self) -> None:
        """Wait for all tasks to be processed."""
        self.queue.join()
    
    def empty(self) -> bool:
        """
        Check if the queue is empty.
        
        Returns:
            True if the queue is empty, False otherwise
        """
        return self.queue.empty()
    
    def size(self) -> int:
        """
        Get the approximate size of the queue.
        
        Returns:
            Approximate size of the queue
        """
        return self.queue.qsize()


class Worker(threading.Thread):
    """Worker thread for processing items from a queue."""
    
    def __init__(
        self,
        queue: WorkQueue,
        processor: Callable,
        name: Optional[str] = None,
        *args,
        **kwargs
    ):
        """
        Initialize the worker.
        
        Args:
            queue: Work queue
            processor: Function to process items
            name: Worker name
            *args: Additional arguments for the processor
            **kwargs: Additional keyword arguments for the processor
        """
        super().__init__(name=name)
        self.queue = queue
        self.processor = processor
        self.args = args
        self.kwargs = kwargs
        self.daemon = True
        self.logger = logging.getLogger(__name__)
        self.running = True
    
    def run(self):
        """Run the worker."""
        while self.running:
            try:
                item = self.queue.get(timeout=1)
                try:
                    self.processor(item, *self.args, **self.kwargs)
                except Exception as e:
                    self.logger.error(f"Error processing item: {str(e)}")
                finally:
                    self.queue.task_done()
            except Exception:
                # Queue.get timed out, check if we should still be running
                pass
    
    def stop(self):
        """Stop the worker."""
        self.running = False


class WorkerPool:
    """Pool of worker threads for parallel processing."""
    
    def __init__(
        self,
        num_workers: int,
        processor: Callable,
        max_queue_size: int = 1000,
        *args,
        **kwargs
    ):
        """
        Initialize the worker pool.
        
        Args:
            num_workers: Number of worker threads
            processor: Function to process items
            max_queue_size: Maximum queue size
            *args: Additional arguments for the processor
            **kwargs: Additional keyword arguments for the processor
        """
        self.queue = WorkQueue(max_size=max_queue_size)
        self.workers = []
        self.logger = logging.getLogger(__name__)
        
        for i in range(num_workers):
            worker = Worker(
                queue=self.queue,
                processor=processor,
                name=f"Worker-{i}",
                *args,
                **kwargs
            )
            self.workers.append(worker)
            worker.start()
        
        self.logger.info(f"Started worker pool with {num_workers} workers")
    
    def add_work(self, item: Any) -> None:
        """
        Add an item to the work queue.
        
        Args:
            item: Item to process
        """
        self.queue.put(item)
    
    def wait_completion(self) -> None:
        """Wait for all work to be completed."""
        self.queue.join()
    
    def shutdown(self) -> None:
        """Shutdown the worker pool."""
        for worker in self.workers:
            worker.stop()
        
        for worker in self.workers:
            worker.join(timeout=1)
        
        self.logger.info("Worker pool shutdown complete")

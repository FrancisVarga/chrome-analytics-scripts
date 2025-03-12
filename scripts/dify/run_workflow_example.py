#!/usr/bin/env python
"""
Example script that demonstrates how to use the DifyClient to trigger a workflow
programmatically without using the command-line interface.

This script shows how to:
1. Initialize the DifyClient
2. Prepare workflow inputs
3. Execute a workflow in both blocking and streaming modes
4. Process the workflow results
"""

import json
import logging
from typing import Dict, Any

from analytics_framework.api.dify_client import DifyClient
from analytics_framework.config import setup_logging, DIFY_API_KEY, DIFY_BASE_URL


def run_workflow_blocking(
    client: DifyClient,
    workflow_id: str,
    additional_inputs: Dict[str, Any] = None
) -> None:
    """
    Run a workflow in blocking mode and print the results.
    
    Args:
        client: Initialized DifyClient
        workflow_id: ID of the workflow to run
        additional_inputs: Additional inputs for the workflow
    """
    # Prepare inputs
    inputs = {
        "workflow_id": workflow_id,
        "query": "Analyze this conversation for key insights"
    }
    
    # Add any additional inputs
    if additional_inputs:
        inputs.update(additional_inputs)
    
    logging.info(f"Running workflow '{workflow_id}' in blocking mode")
    logging.info(f"Inputs: {json.dumps(inputs, indent=2)}")
    
    # Execute workflow
    result = client.execute_workflow(
        inputs=inputs,
        response_mode="blocking",
        user_id="example_user"
    )
    
    # Process results
    logging.info(f"Workflow execution ID: {result.get('workflow_run_id')}")
    
    if "data" in result:
        data = result["data"]
        logging.info(f"Status: {data.get('status')}")
        
        if "outputs" in data:
            print("\nWorkflow outputs:")
            print(json.dumps(data["outputs"], indent=2))
        
        if "elapsed_time" in data:
            logging.info(f"Elapsed time: {data['elapsed_time']} seconds")
        
        if "total_tokens" in data:
            logging.info(f"Total tokens: {data['total_tokens']}")


def run_workflow_streaming(
    client: DifyClient,
    workflow_id: str,
    additional_inputs: Dict[str, Any] = None
) -> None:
    """
    Run a workflow in streaming mode and print the results.
    
    Args:
        client: Initialized DifyClient
        workflow_id: ID of the workflow to run
        additional_inputs: Additional inputs for the workflow
    """
    # Prepare inputs
    inputs = {
        "workflow_id": workflow_id,
        "query": "Analyze this conversation for key insights"
    }
    
    # Add any additional inputs
    if additional_inputs:
        inputs.update(additional_inputs)
    
    logging.info(f"Running workflow '{workflow_id}' in streaming mode")
    logging.info(f"Inputs: {json.dumps(inputs, indent=2)}")
    
    print("\nStreaming response:")
    
    # Execute workflow
    for chunk in client.execute_workflow(
        inputs=inputs,
        response_mode="streaming",
        user_id="example_user"
    ):
        event = chunk.get("event")
        
        if event == "workflow_started":
            logging.info(f"Workflow started: {chunk.get('id')}")
        elif event == "workflow_finished":
            logging.info(f"Workflow finished: {chunk.get('id')}")
            if "outputs" in chunk:
                print("\nFinal outputs:")
                print(json.dumps(chunk["outputs"], indent=2))
        elif event == "message":
            # Print message content
            if "answer" in chunk:
                print(chunk["answer"], end="", flush=True)
    
    # Print newline after streaming
    print()


def main() -> None:
    """Run the example workflow."""
    # Set up logging
    setup_logging()
    logging.getLogger().setLevel(logging.INFO)
    
    # Check if API key is available
    if not DIFY_API_KEY:
        logging.error("Dify API key not found. Please set DIFY_API_KEY environment variable.")
        return
    
    # Initialize client
    client = DifyClient(api_key=DIFY_API_KEY, base_url=DIFY_BASE_URL)
    
    # Example workflow ID
    workflow_id = "sample_workflow"
    
    # Example additional inputs
    additional_inputs = {
        "conversation_id": "conv_12345",
        "language": "en",
        "max_tokens": 1000,
        "temperature": 0.7
    }
    
    # Run in blocking mode
    try:
        run_workflow_blocking(client, workflow_id, additional_inputs)
    except Exception as e:
        logging.error(f"Error running workflow in blocking mode: {str(e)}")
    
    print("\n" + "-" * 50 + "\n")
    
    # Run in streaming mode
    try:
        run_workflow_streaming(client, workflow_id, additional_inputs)
    except Exception as e:
        logging.error(f"Error running workflow in streaming mode: {str(e)}")


if __name__ == "__main__":
    main()

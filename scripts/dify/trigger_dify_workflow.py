#!/usr/bin/env python
"""
Script to trigger a Dify workflow.

This script uses the DifyClient to execute a Dify workflow with the specified inputs.
It supports both blocking and streaming response modes.

Usage:
    python scripts/dify/trigger_dify_workflow.py --input-key query=value
    python scripts/dify/trigger_dify_workflow.py --input-key query=value --stream
    python scripts/dify/trigger_dify_workflow.py --input-file inputs.json
"""

import argparse
import json
import logging
import sys
from typing import Dict, Any

from analytics_framework.api.dify_client import DifyClient
from analytics_framework.config import setup_logging, DIFY_API_KEY, DIFY_BASE_URL


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Trigger a Dify workflow")
    
    # Input methods (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input-file",
        help="JSON file containing workflow inputs"
    )
    input_group.add_argument(
        "--input-key",
        action="append",
        help="Key-value pairs for workflow inputs (can be used multiple times)"
    )
    
    # Optional arguments
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Use streaming response mode instead of blocking"
    )
    parser.add_argument(
        "--user-id",
        default="system_analytics",
        help="User identifier for the workflow execution"
    )
    parser.add_argument(
        "--api-key",
        help="Dify API key (overrides environment variable)"
    )
    parser.add_argument(
        "--base-url",
        help="Dify API base URL (overrides environment variable)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    return parser.parse_args()


def parse_input_keys(input_keys: list) -> Dict[str, Any]:
    """
    Parse input key-value pairs from command line arguments.
    
    Args:
        input_keys: List of key=value strings
        
    Returns:
        Dictionary of input key-value pairs
    """
    inputs = {}
    
    for key_value in input_keys:
        if "=" not in key_value:
            raise ValueError(f"Invalid input format: {key_value}. Expected format: key=value")
        
        key, value = key_value.split("=", 1)
        
        # Try to parse as JSON if it looks like a JSON value
        if value.startswith(("{", "[", "\"", "true", "false", "null")) or value.isdigit():
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                # Keep as string if not valid JSON
                pass
        
        inputs[key] = value
    
    return inputs


def load_inputs_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load workflow inputs from a JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary of input key-value pairs
    """
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        raise ValueError(f"Error loading inputs from file: {str(e)}")


def trigger_workflow(args: argparse.Namespace) -> None:
    """
    Trigger a Dify workflow with the specified inputs.
    
    Args:
        args: Command line arguments
    """
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.getLogger().setLevel(log_level)
    
    # Get API credentials
    api_key = args.api_key or DIFY_API_KEY
    base_url = args.base_url or DIFY_BASE_URL
    
    if not api_key:
        logging.error("Dify API key not provided. Set DIFY_API_KEY environment variable or use --api-key")
        sys.exit(1)
    
    if not base_url:
        logging.error("Dify base URL not provided. Set DIFY_BASE_URL environment variable or use --base-url")
        sys.exit(1)
    
    # Get workflow inputs
    if args.input_file:
        try:
            inputs = load_inputs_from_file(args.input_file)
        except ValueError as e:
            logging.error(str(e))
            sys.exit(1)
    else:
        try:
            inputs = parse_input_keys(args.input_key)
        except ValueError as e:
            logging.error(str(e))
            sys.exit(1)
    
    # Response mode
    response_mode = "streaming" if args.stream else "blocking"
    
    # Initialize Dify client
    client = DifyClient(api_key=api_key, base_url=base_url)
    
    try:
        logging.info(f"Triggering Dify workflow with inputs: {json.dumps(inputs, indent=2)}")
        
        if response_mode == "blocking":
            # Execute workflow in blocking mode
            result = client.execute_workflow(
                inputs=inputs,
                response_mode=response_mode,
                user_id=args.user_id
            )
            
            # Print workflow result
            logging.info(f"Workflow execution ID: {result.get('workflow_run_id')}")
            logging.info(f"Task ID: {result.get('task_id')}")
            
            # Print data if available
            if "data" in result:
                data = result["data"]
                logging.info(f"Status: {data.get('status')}")
                
                if "outputs" in data:
                    print(json.dumps(data["outputs"], indent=2))
                elif "error" in data:
                    logging.error(f"Workflow error: {data['error']}")
                
                if "elapsed_time" in data:
                    logging.info(f"Elapsed time: {data['elapsed_time']} seconds")
                
                if "total_tokens" in data:
                    logging.info(f"Total tokens: {data['total_tokens']}")
        else:
            # Execute workflow in streaming mode
            for chunk in client.execute_workflow(
                inputs=inputs,
                response_mode=response_mode,
                user_id=args.user_id
            ):
                event = chunk.get("event")
                
                if event == "workflow_started":
                    logging.info(f"Workflow started: {chunk.get('id')}")
                elif event == "node_started":
                    logging.info(f"Node started: {chunk.get('node_id')}")
                elif event == "node_finished":
                    logging.info(f"Node finished: {chunk.get('node_id')}")
                elif event == "workflow_finished":
                    logging.info(f"Workflow finished: {chunk.get('id')}")
                    if "outputs" in chunk:
                        print(json.dumps(chunk["outputs"], indent=2))
                    if "error" in chunk:
                        logging.error(f"Workflow error: {chunk['error']}")
                elif event == "message":
                    # Print message content
                    if "answer" in chunk:
                        print(chunk["answer"], end="", flush=True)
                elif event != "ping":
                    # Print other events for debugging
                    if args.verbose:
                        logging.debug(f"Event: {json.dumps(chunk)}")
            
            # Print newline after streaming
            print()
    
    except Exception as e:
        logging.error(f"Error executing workflow: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    setup_logging()
    args = parse_arguments()
    trigger_workflow(args)

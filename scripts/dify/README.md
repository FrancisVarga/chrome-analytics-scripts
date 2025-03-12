# Dify Workflow Scripts

This directory contains scripts for interacting with the Dify Workflow API. These scripts allow you to trigger Dify workflows with custom inputs and process the results.

## Prerequisites

Before using these scripts, make sure you have:

1. Set up the required environment variables in your `.env` file:

   ```
   DIFY_API_KEY=your_api_key_here
   DIFY_BASE_URL=https://your-dify-instance.com/v1
   DIFY_WORKFLOW_ENABLED=true
   ```

2. Installed the required dependencies (already included in the project's `requirements.txt`)

## Available Scripts

### 1. Command-Line Interface: `trigger_dify_workflow.py`

This script provides a flexible command-line interface for triggering Dify workflows.

#### Usage Examples

**Basic usage with key-value inputs:**

```bash
python scripts/dify/trigger_dify_workflow.py --workflow-id sample_workflow --input-key query="Analyze this conversation"
```

**Multiple input parameters:**

```bash
python scripts/dify/trigger_dify_workflow.py --workflow-id sample_workflow \
  --input-key query="Analyze this conversation" \
  --input-key conversation_id=conv_12345 \
  --input-key language=en
```

**Using a JSON file for inputs:**

```bash
python scripts/dify/trigger_dify_workflow.py --workflow-id sample_workflow --input-file scripts/dify/sample_workflow_inputs.json
```

**Streaming mode:**

```bash
python scripts/dify/trigger_dify_workflow.py --workflow-id sample_workflow --input-key query="Analyze this conversation" --stream
```

**Overriding API credentials:**

```bash
python scripts/dify/trigger_dify_workflow.py --workflow-id sample_workflow \
  --input-key query="Analyze this conversation" \
  --api-key your_api_key \
  --base-url https://your-dify-instance.com/v1
```

**Verbose logging:**

```bash
python scripts/dify/trigger_dify_workflow.py --workflow-id sample_workflow --input-key query="Analyze this conversation" --verbose
```

### 2. Programmatic Example: `run_workflow_example.py`

This script demonstrates how to use the `DifyClient` class programmatically in your Python code. It shows:

- How to initialize the client
- How to prepare workflow inputs
- How to execute workflows in both blocking and streaming modes
- How to process workflow results

Run the example:

```bash
python scripts/dify/run_workflow_example.py
```

## Sample Workflow Inputs

The `sample_workflow_inputs.json` file provides an example of workflow inputs in JSON format:

```json
{
    "workflow_id": "sample_workflow",
    "query": "What is the sentiment analysis of this conversation?",
    "conversation_id": "conv_12345",
    "language": "en",
    "max_tokens": 1000,
    "temperature": 0.7
}
```

## Integrating with Your Code

To integrate Dify workflows into your own code:

```python
from analytics_framework.api.dify_client import DifyClient
from analytics_framework.config import DIFY_API_KEY, DIFY_BASE_URL

# Initialize client
client = DifyClient(api_key=DIFY_API_KEY, base_url=DIFY_BASE_URL)

# Prepare inputs
inputs = {
    "workflow_id": "your_workflow_id",
    "query": "Your query here",
    # Additional parameters as needed
}

# Execute workflow (blocking mode)
result = client.execute_workflow(
    inputs=inputs,
    response_mode="blocking",
    user_id="your_user_id"
)

# Process results
print(f"Workflow execution ID: {result.get('workflow_run_id')}")
if "data" in result and "outputs" in result["data"]:
    print(result["data"]["outputs"])
```

## Error Handling

The scripts include comprehensive error handling:

- API connection errors
- Invalid input formats
- Workflow execution errors
- Missing credentials

If you encounter issues, use the `--verbose` flag with the CLI script to get more detailed logging information.

## Limitations

- Streaming mode may time out for long-running workflows
- File uploads are supported but not demonstrated in these examples
- The Dify API may have rate limits depending on your subscription

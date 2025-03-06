"""Track and persist processing state for resumable operations."""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List


class ProcessingState:
    """Track and persist processing state for resumable operations."""
    
    def __init__(self, state_file_path: str = "processing_state.json"):
        """
        Initialize the processing state tracker.
        
        Args:
            state_file_path: Path to the state file
        """
        self.state_file_path = state_file_path
        self.logger = logging.getLogger(__name__)
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """
        Load state from file.
        
        Returns:
            State dictionary
        """
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading state file: {str(e)}")
                return self._initialize_state()
        else:
            return self._initialize_state()
    
    def _initialize_state(self) -> Dict[str, Any]:
        """
        Initialize a new state.
        
        Returns:
            New state dictionary
        """
        return {
            "last_processed_conversation_id": None,
            "last_processed_timestamp": None,
            "processed_count": 0,
            "last_run_start_time": None,
            "last_run_end_time": None,
            "runs": [],
            "errors": []
        }
    
    def save(self) -> None:
        """Save the current state to file."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(self.state_file_path)), exist_ok=True)
            
            with open(self.state_file_path, 'w') as f:
                json.dump(self.state, f, indent=2)
            self.logger.debug(f"State saved to {self.state_file_path}")
        except Exception as e:
            self.logger.error(f"Error saving state file: {str(e)}")
    
    def update_last_processed(self, conversation_id: str, timestamp: Optional[str] = None) -> None:
        """
        Update the last processed conversation.
        
        Args:
            conversation_id: ID of the last processed conversation
            timestamp: Timestamp of the conversation (optional)
        """
        self.state["last_processed_conversation_id"] = conversation_id
        self.state["last_processed_timestamp"] = timestamp or datetime.now().isoformat()
        self.state["processed_count"] += 1
        
        # Auto-save every 10 updates
        if self.state["processed_count"] % 10 == 0:
            self.save()
    
    def start_run(self) -> None:
        """Mark the start of a processing run."""
        start_time = datetime.now().isoformat()
        self.state["last_run_start_time"] = start_time
        self.state["runs"].append({
            "start_time": start_time,
            "end_time": None,
            "processed_count": 0,
            "status": "running"
        })
        self.save()
    
    def end_run(self, success: bool = True, message: Optional[str] = None) -> None:
        """
        Mark the end of a processing run.
        
        Args:
            success: Whether the run was successful
            message: Optional message about the run
        """
        end_time = datetime.now().isoformat()
        self.state["last_run_end_time"] = end_time
        
        if self.state["runs"]:
            current_run = self.state["runs"][-1]
            current_run["end_time"] = end_time
            current_run["processed_count"] = self.state["processed_count"] - sum(
                run.get("processed_count", 0) for run in self.state["runs"][:-1]
            )
            current_run["status"] = "completed" if success else "failed"
            if message:
                current_run["message"] = message
        
        self.save()
    
    def record_error(self, error_message: str, conversation_id: Optional[str] = None) -> None:
        """
        Record an error.
        
        Args:
            error_message: Error message
            conversation_id: ID of the conversation that caused the error (optional)
        """
        self.state["errors"].append({
            "timestamp": datetime.now().isoformat(),
            "conversation_id": conversation_id,
            "error_message": error_message
        })
        
        # Keep only the last 100 errors
        if len(self.state["errors"]) > 100:
            self.state["errors"] = self.state["errors"][-100:]
        
        self.save()
    
    def get_last_processed_id(self) -> Optional[str]:
        """
        Get the ID of the last processed conversation.
        
        Returns:
            Last processed conversation ID or None
        """
        return self.state.get("last_processed_conversation_id")
    
    def get_processed_count(self) -> int:
        """
        Get the total number of processed conversations.
        
        Returns:
            Processed count
        """
        return self.state.get("processed_count", 0)
    
    def get_run_stats(self) -> Dict[str, Any]:
        """
        Get statistics about processing runs.
        
        Returns:
            Run statistics
        """
        runs = self.state.get("runs", [])
        total_runs = len(runs)
        successful_runs = sum(1 for run in runs if run.get("status") == "completed")
        failed_runs = sum(1 for run in runs if run.get("status") == "failed")
        
        return {
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "last_run_start": self.state.get("last_run_start_time"),
            "last_run_end": self.state.get("last_run_end_time"),
            "current_run_status": runs[-1].get("status") if runs else None
        }
    
    def get_errors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent errors.
        
        Args:
            limit: Maximum number of errors to return
            
        Returns:
            List of recent errors
        """
        errors = self.state.get("errors", [])
        return errors[-limit:] if errors else []
    
    def reset(self) -> None:
        """Reset the processing state."""
        self.state = self._initialize_state()
        self.save()


class S3ProcessingState:
    """Track and persist processing state in S3 for distributed operations."""
    
    def __init__(
        self,
        bucket: str,
        key_prefix: str = "processing_state",
        region_name: Optional[str] = None
    ):
        """
        Initialize the S3 processing state tracker.
        
        Args:
            bucket: S3 bucket name
            key_prefix: Prefix for state keys
            region_name: AWS region name
        """
        self.bucket = bucket
        self.key_prefix = key_prefix
        self.state_key = f"{key_prefix}/current_state.json"
        self.logger = logging.getLogger(__name__)
        
        # Initialize S3 client
        try:
            import boto3
            self.s3_client = boto3.client('s3', region_name=region_name)
            self.s3_available = True
        except ImportError:
            self.logger.warning("boto3 not available, S3 state tracking disabled")
            self.s3_available = False
        except Exception as e:
            self.logger.error(f"Error initializing S3 client: {str(e)}")
            self.s3_available = False
        
        # Load initial state
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """
        Load state from S3.
        
        Returns:
            State dictionary
        """
        if not self.s3_available:
            return self._initialize_state()
            
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self.state_key)
            state_json = response['Body'].read().decode('utf-8')
            return json.loads(state_json)
        except Exception as e:
            self.logger.info(f"No state file found in S3 or error loading it: {str(e)}")
            return self._initialize_state()
    
    def _initialize_state(self) -> Dict[str, Any]:
        """
        Initialize a new state.
        
        Returns:
            New state dictionary
        """
        return {
            "last_processed_conversation_id": None,
            "last_processed_timestamp": None,
            "processed_count": 0,
            "last_run_start_time": None,
            "last_run_end_time": None,
            "runs": [],
            "errors": []
        }
    
    def save(self) -> None:
        """Save the current state to S3."""
        if not self.s3_available:
            self.logger.warning("S3 not available, state not saved")
            return
            
        try:
            state_json = json.dumps(self.state)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self.state_key,
                Body=state_json,
                ContentType='application/json'
            )
            
            # Also save a timestamped backup
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            backup_key = f"{self.key_prefix}/backups/state_{timestamp}.json"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=backup_key,
                Body=state_json,
                ContentType='application/json'
            )
            
            self.logger.debug(f"State saved to s3://{self.bucket}/{self.state_key}")
        except Exception as e:
            self.logger.error(f"Error saving state to S3: {str(e)}")
    
    def update_last_processed(self, conversation_id: str, timestamp: Optional[str] = None) -> None:
        """
        Update the last processed conversation.
        
        Args:
            conversation_id: ID of the last processed conversation
            timestamp: Timestamp of the conversation (optional)
        """
        self.state["last_processed_conversation_id"] = conversation_id
        self.state["last_processed_timestamp"] = timestamp or datetime.now().isoformat()
        self.state["processed_count"] += 1
        
        # Auto-save every 10 updates
        if self.state["processed_count"] % 10 == 0:
            self.save()
    
    def start_run(self) -> None:
        """Mark the start of a processing run."""
        start_time = datetime.now().isoformat()
        self.state["last_run_start_time"] = start_time
        self.state["runs"].append({
            "start_time": start_time,
            "end_time": None,
            "processed_count": 0,
            "status": "running"
        })
        self.save()
    
    def end_run(self, success: bool = True, message: Optional[str] = None) -> None:
        """
        Mark the end of a processing run.
        
        Args:
            success: Whether the run was successful
            message: Optional message about the run
        """
        end_time = datetime.now().isoformat()
        self.state["last_run_end_time"] = end_time
        
        if self.state["runs"]:
            current_run = self.state["runs"][-1]
            current_run["end_time"] = end_time
            current_run["processed_count"] = self.state["processed_count"] - sum(
                run.get("processed_count", 0) for run in self.state["runs"][:-1]
            )
            current_run["status"] = "completed" if success else "failed"
            if message:
                current_run["message"] = message
        
        self.save()
    
    def record_error(self, error_message: str, conversation_id: Optional[str] = None) -> None:
        """
        Record an error.
        
        Args:
            error_message: Error message
            conversation_id: ID of the conversation that caused the error (optional)
        """
        self.state["errors"].append({
            "timestamp": datetime.now().isoformat(),
            "conversation_id": conversation_id,
            "error_message": error_message
        })
        
        # Keep only the last 100 errors
        if len(self.state["errors"]) > 100:
            self.state["errors"] = self.state["errors"][-100:]
        
        self.save()
    
    def get_last_processed_id(self) -> Optional[str]:
        """
        Get the ID of the last processed conversation.
        
        Returns:
            Last processed conversation ID or None
        """
        return self.state.get("last_processed_conversation_id")
    
    def get_processed_count(self) -> int:
        """
        Get the total number of processed conversations.
        
        Returns:
            Processed count
        """
        return self.state.get("processed_count", 0)
    
    def get_run_stats(self) -> Dict[str, Any]:
        """
        Get statistics about processing runs.
        
        Returns:
            Run statistics
        """
        runs = self.state.get("runs", [])
        total_runs = len(runs)
        successful_runs = sum(1 for run in runs if run.get("status") == "completed")
        failed_runs = sum(1 for run in runs if run.get("status") == "failed")
        
        return {
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "last_run_start": self.state.get("last_run_start_time"),
            "last_run_end": self.state.get("last_run_end_time"),
            "current_run_status": runs[-1].get("status") if runs else None
        }
    
    def get_errors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent errors.
        
        Args:
            limit: Maximum number of errors to return
            
        Returns:
            List of recent errors
        """
        errors = self.state.get("errors", [])
        return errors[-limit:] if errors else []
    
    def reset(self) -> None:
        """Reset the processing state."""
        self.state = self._initialize_state()
        self.save()


def create_processing_state(
    use_s3: bool = False,
    state_file_path: str = "processing_state.json",
    s3_bucket: Optional[str] = None,
    s3_key_prefix: str = "processing_state",
    region_name: Optional[str] = None
) -> ProcessingState:
    """
    Create a processing state tracker.
    
    Args:
        use_s3: Whether to use S3 for state tracking
        state_file_path: Path to the state file (for local state tracking)
        s3_bucket: S3 bucket name (for S3 state tracking)
        s3_key_prefix: Prefix for S3 state keys (for S3 state tracking)
        region_name: AWS region name (for S3 state tracking)
        
    Returns:
        Processing state tracker
    """
    if use_s3 and s3_bucket:
        return S3ProcessingState(
            bucket=s3_bucket,
            key_prefix=s3_key_prefix,
            region_name=region_name
        )
    else:
        return ProcessingState(state_file_path=state_file_path)

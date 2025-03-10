#!/usr/bin/env python
"""Command-line interface for the analytics framework."""

import argparse
import sys
from datetime import datetime

from analytics_framework.main import main as analytics_main


def parse_date(date_str):
    """Parse date string in various formats."""
    if not date_str:
        return None
        
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).isoformat()
        except ValueError:
            continue
    
    raise ValueError(f"Unsupported date format: {date_str}")


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description='Conversation Analytics Framework')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Collect command
    collect_parser = subparsers.add_parser('collect', help='Collect and process conversation data')
    collect_parser.add_argument('--start-date', type=str, help='Start date for data collection (YYYY-MM-DD)')
    collect_parser.add_argument('--end-date', type=str, help='End date for data collection (YYYY-MM-DD)')
    collect_parser.add_argument('--app-id', type=str, help='App ID for filtering conversations')
    collect_parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    collect_parser.add_argument('--no-mongodb', action='store_true', help='Skip MongoDB storage')
    collect_parser.add_argument('--no-parquet', action='store_true', help='Skip Parquet storage')
    collect_parser.add_argument('--resume', action='store_true', help='Resume from last processed conversation')
    collect_parser.add_argument('--state-file', type=str, default='processing_state.json', help='Path to state file')
    collect_parser.add_argument('--use-s3-state', action='store_true', help='Use S3 for state tracking')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show processing status')
    status_parser.add_argument('--state-file', type=str, default='processing_state.json', help='Path to state file')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Handle commands
    if args.command == 'collect':
        # Parse dates
        start_date = parse_date(args.start_date) if args.start_date else None
        end_date = parse_date(args.end_date) if args.end_date else None
        
        # Set command-line arguments for the main function
        sys.argv = [
            'analytics_framework',
            '--batch-size', str(args.batch_size)
        ]
        
        if start_date:
            sys.argv.extend(['--start-date', start_date])
            
        if end_date:
            sys.argv.extend(['--end-date', end_date])
            
        if args.app_id:
            sys.argv.extend(['--app-id', args.app_id])
            
        if args.no_mongodb:
            sys.argv.append('--no-mongodb')
            
        if args.no_parquet:
            sys.argv.append('--no-parquet')
            
        if args.resume:
            sys.argv.append('--resume')
            
        if args.state_file:
            sys.argv.extend(['--state-file', args.state_file])
            
        if args.use_s3_state:
            sys.argv.append('--use-s3-state')
        
        # Run the main function
        analytics_main()
        
    elif args.command == 'status':
        # Import here to avoid circular imports
        from analytics_framework.utils.processing_state import ProcessingState
        
        # Load state
        state = ProcessingState(args.state_file)
        
        # Print status
        print("\n=== PROCESSING STATE SUMMARY ===\n")
        
        # Basic stats
        last_id = state.get_last_processed_id() or "N/A"
        processed_count = state.get_processed_count()
        
        print(f"Last Processed ID: {last_id}")
        print(f"Total Processed: {processed_count:,} conversations")
        
        # Run stats
        run_stats = state.get_run_stats()
        print(f"\nTotal Runs: {run_stats['total_runs']}")
        print(f"Successful Runs: {run_stats['successful_runs']}")
        print(f"Failed Runs: {run_stats['failed_runs']}")
        
        if run_stats['last_run_start']:
            print(f"Last Run Start: {run_stats['last_run_start']}")
            
        if run_stats['last_run_end']:
            print(f"Last Run End: {run_stats['last_run_end']}")
            
        if run_stats['current_run_status']:
            print(f"Current Run Status: {run_stats['current_run_status']}")
        
        # Recent errors
        errors = state.get_errors(5)
        if errors:
            print("\n=== RECENT ERRORS ===\n")
            for i, error in enumerate(errors):
                print(f"{i+1}. [{error.get('timestamp', 'N/A')}] {error.get('error_message', 'Unknown error')}")
                if error.get('conversation_id'):
                    print(f"   Conversation ID: {error['conversation_id']}")
                print()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

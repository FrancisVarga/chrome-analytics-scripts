# Analytics Framework Architecture Plan

## Overview

This document outlines the architecture and design decisions for the Conversation Analytics Framework. The framework is designed to collect conversation data from NocoDB, process it, and store it in both MongoDB and S3 (as Parquet files) for efficient analytics.

## System Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│   NocoDB API    │────▶│  Data Processor │────▶│  MongoDB        │
│   (Data Source) │     │                 │     │  (Operational)  │
│                 │     │                 │     │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 │
                                 ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │                 │     │                 │
                        │  Categorization │────▶│  S3 Parquet     │
                        │  Engine         │     │  (Analytics)    │
                        │                 │     │                 │
                        └─────────────────┘     └─────────────────┘
                                                        │
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │                 │
                                               │  Analytics      │
                                               │  Reports        │
                                               │                 │
                                               └─────────────────┘
```

## Key Components

### 1. Data Collection

- **NocoDB API Client**: Connects to NocoDB API to fetch conversation and message data
- **Processing State Tracker**: Keeps track of which conversations have been processed
- **Incremental Processing**: Only processes new or updated conversations

### 2. Data Processing

- **Conversation Processor**: Transforms raw conversation data into analytics-ready format
- **Message Processor**: Processes individual messages within conversations
- **Multi-threading Support**: Parallel processing for improved performance
- **GPU Acceleration**: Optional GPU support for computationally intensive tasks

### 3. Data Storage

- **MongoDB Storage**: Primary operational database for flexible querying
  - Optimized schema for analytics queries
  - Indexed for efficient retrieval
- **S3 Parquet Storage**: Columnar storage for efficient analytics
  - Partitioned by date for query performance
  - Optimized file sizes and compression
  - Designed for integration with analytics tools

### 4. Analytics Engine

- **Categorization Engine**: Automatically categorizes conversations
  - Topic detection
  - Intent recognition
  - Sentiment analysis
- **Analytics Aggregation**: Pre-computes common analytics metrics
  - Daily/weekly/monthly reports
  - User-based analytics
  - Model performance metrics

## Database Schema

### MongoDB Collections

1. **conversation_analytics**: Merged conversation data with analytics fields
2. **conversation_messages**: Individual messages within conversations
3. **conversation_categories**: Categorization data for conversations
4. **conversation_translations**: Translation data for conversations/messages
5. **analytics_reports**: Pre-computed analytics reports
6. **user_analytics**: User-specific analytics data

### S3 Parquet Structure

```
s3://bucket/
  ├── conversations/
  │   ├── year=2025/
  │   │   ├── month=03/
  │   │   │   ├── day=01/
  │   │   │   │   ├── conversations.parquet
  │   │   │   │   ├── messages/
  │   │   │   │   │   └── messages.parquet
  │   │   │   │   └── categories/
  │   │   │   │       └── categories.parquet
  │   │   │   └── day=02/
  │   │   │       └── ...
  │   │   └── month=04/
  │   │       └── ...
  │   └── year=2024/
  │       └── ...
  ├── user_analytics/
  │   └── user_analytics.parquet
  └── analytics_reports/
      ├── daily/
      │   └── ...
      ├── weekly/
      │   └── ...
      └── monthly/
          └── ...
```

## Processing Flow

1. **Data Collection**:
   - Fetch conversations from NocoDB API
   - Track last processed conversation ID
   - Handle pagination for large datasets

2. **Data Processing**:
   - Transform conversations and messages
   - Extract metadata for analytics
   - Compute derived metrics

3. **Categorization**:
   - Analyze conversation content
   - Apply categorization rules/models
   - Assign categories with confidence scores

4. **Storage**:
   - Store in MongoDB for operational use
   - Write to S3 as Parquet files for analytics
   - Update processing state

5. **Analytics Generation**:
   - Compute aggregated metrics
   - Generate periodic reports
   - Update user analytics

## Scalability Considerations

- **Horizontal Scaling**: Multiple workers can process different batches
- **Vertical Scaling**: GPU acceleration for intensive operations
- **Incremental Processing**: Only process new or changed data
- **Optimized Storage**: Efficient data formats and compression
- **Partitioning**: Data partitioning for query performance

## Security Considerations

- **Environment Variables**: Secure configuration via .env files
- **API Authentication**: Secure access to NocoDB API
- **S3 Security**: Proper IAM roles and bucket policies
- **MongoDB Security**: Authentication and network security
- **Data Privacy**: Optional anonymization for sensitive data

## Monitoring and Maintenance

- **Processing State**: Track processing progress and errors
- **Logging**: Comprehensive logging for debugging
- **Error Handling**: Robust error recovery mechanisms
- **Performance Metrics**: Monitor processing speed and resource usage

## Implementation Approach

The implementation will follow a modular, component-based approach:

1. **Core Infrastructure**: Basic project setup, configuration, and utilities
2. **Data Collection**: NocoDB API client and processing state tracking
3. **Data Processing**: Conversation and message processing logic
4. **Storage Implementation**: MongoDB and S3 Parquet storage
5. **Analytics Engine**: Categorization and analytics computation
6. **API Layer**: REST API for accessing analytics data
7. **Deployment**: Containerization and deployment automation

Each component will be developed and tested independently, with integration tests to ensure proper interaction between components.

## Development Notes

### Dependencies

- Several dependencies related to the S3 Parquet storage module (pandas, pyarrow, fastparquet, dask, distributed) are commented out in requirements.txt until the module is implemented.
- When implementing the S3 Parquet storage module, uncomment these dependencies in requirements.txt.
- Note that fastparquet requires Rust to be installed on the system for building the cramjam dependency.

### Development Workflow

- A pre-commit hook is implemented to automatically sync requirements.txt with uv whenever Python files or dependency-related files are changed.
- The pre-commit hook preserves comments and structure in requirements.txt while updating dependency versions.
- Installation scripts are provided for both Windows (PowerShell) and Unix-like systems (Bash).
- To install the pre-commit hook, run `.\install-hooks.ps1` (Windows) or `./install-hooks.sh` (macOS/Linux/Git Bash).

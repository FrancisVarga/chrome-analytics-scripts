# System Patterns: Conversation Analytics Framework

## Architecture Overview

The Conversation Analytics Framework follows a modular, pipeline-based architecture designed for scalability, performance, and maintainability. The system is organized into distinct components that handle specific aspects of the data flow.

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

### 1. API Clients

**NocoDB API Client** (`analytics_framework/api/nocodb_client.py`)

- Responsible for fetching conversation and message data from NocoDB
- Handles authentication, pagination, and error handling
- Implements filtering and sorting capabilities
- Provides a clean abstraction over the NocoDB REST API

**Design Patterns:**

- Repository Pattern: Abstracts data access logic
- Adapter Pattern: Converts external API data to internal models
- Retry Pattern: Implements automatic retries for transient failures

### 2. Data Models

**MongoDB Schema** (`analytics_framework/models/mongodb_schema.py`)

- Defines the structure of data stored in MongoDB
- Implements validation rules for data integrity
- Provides Python classes for object-oriented data manipulation
- Defines indexes for query optimization

**Design Patterns:**

- Data Transfer Object (DTO): Facilitates data transfer between layers
- Builder Pattern: Constructs complex objects step by step
- Validator Pattern: Ensures data integrity

### 3. Data Processing

**Data Processor** (`analytics_framework/processors/data_processor.py`)

- Transforms raw conversation data into analytics-ready format
- Computes derived metrics and aggregations
- Orchestrates the processing pipeline
- Handles error recovery and state management

**Design Patterns:**

- Pipeline Pattern: Processes data through a series of transformations
- Strategy Pattern: Allows for different processing strategies
- Observer Pattern: Notifies components of processing events

### 4. Storage

**MongoDB Client Structure** (`analytics_framework/storage/mongodb/`)

- **Base Client** (`base_client.py`): Provides common MongoDB operations
  - Connection management and pooling
  - CRUD operations for all collections
  - Bulk operations for efficiency
  - Aggregation pipelines
  - Error handling and retry logic

- **Conversation Client** (`conversation_client.py`): Handles conversation-specific operations
  - Saving and retrieving conversations
  - Managing conversation categories
  - Adding messages to conversations
  - Updating conversation metrics and status

- **Analytics Client** (`analytics_client.py`): Manages analytics operations
  - Saving and retrieving analytics reports
  - Managing user analytics
  - Updating metrics and distributions
  - Generating top user reports

- **Translation Client** (`translation_client.py`): Handles translation operations
  - Saving and retrieving translations
  - Managing language-specific operations
  - Tracking translation statistics

- **Main Client** (`client.py`): Combines all specialized clients
  - Provides a unified interface to all MongoDB operations
  - Manages shared resources and connections

**Parquet Storage** (`analytics_framework/storage/parquet_storage.py`)

- Manages storage of data in Parquet format on S3 or local filesystem
- Implements partitioning strategies for efficient querying
- Optimizes file sizes and compression
- Handles incremental updates

**Design Patterns:**

- Repository Pattern: Abstracts storage details
- Factory Pattern: Creates appropriate storage instances
- Decorator Pattern: Adds caching and other enhancements
- Facade Pattern: Provides a simplified interface to complex subsystems
- Composite Pattern: Combines multiple clients into a unified interface

### 5. Utilities

**Processing State** (`analytics_framework/utils/processing_state.py`)

- Tracks the progress of data processing
- Persists state to enable resumable processing
- Provides checkpointing capabilities
- Implements state recovery mechanisms

**Thread Pool** (`analytics_framework/utils/thread_pool.py`)

- Manages worker threads for parallel processing
- Implements thread-safe operations
- Provides resource management and throttling
- Handles task distribution and collection

**Design Patterns:**

- Singleton Pattern: Ensures single instance of state manager
- Worker Pool Pattern: Manages concurrent execution
- Command Pattern: Encapsulates processing tasks

## Data Flow

1. **Data Collection**
   - NocoDB API client fetches conversation data
   - Data is validated against expected schema
   - Processing state is updated to track progress

2. **Data Processing**
   - Raw data is transformed into analytics models
   - Derived metrics are computed
   - Data is enriched with additional context

3. **Categorization**
   - Conversations are analyzed for topics, intents, and sentiment
   - Categories are assigned with confidence scores
   - Categorization results are stored with the conversation data

4. **Storage**
   - Processed data is stored in MongoDB for operational use
   - Data is also written to S3 as Parquet files for analytics
   - Indexes and partitioning optimize query performance

5. **Analytics Generation**
   - Pre-computed analytics are generated from processed data
   - Reports are created for different time periods
   - User-specific analytics are updated

## Design Decisions

### Modular Architecture

The framework is designed with clear separation of concerns, allowing components to be developed, tested, and maintained independently. This modular approach enables:

- **Extensibility**: New components can be added without modifying existing ones
- **Testability**: Components can be tested in isolation
- **Maintainability**: Changes to one component don't affect others
- **Reusability**: Components can be reused in different contexts

### Dual Storage Strategy

The framework uses both MongoDB and S3 Parquet for storage, leveraging the strengths of each:

- **MongoDB**: Provides flexible schema, rich querying, and efficient single-record operations
- **S3 Parquet**: Offers columnar storage for analytics, efficient compression, and partitioning

This dual approach optimizes for both operational and analytical use cases.

### Parallel Processing

The framework implements multi-threading to improve performance:

- **Thread Pool**: Manages worker threads for parallel execution
- **Task Distribution**: Divides work into manageable chunks
- **Resource Management**: Controls resource utilization
- **Synchronization**: Ensures thread-safe operations

### State Management

The processing state tracking system ensures reliability:

- **Checkpointing**: Records progress at regular intervals
- **Resumability**: Allows processing to resume from the last checkpoint
- **Error Recovery**: Handles failures gracefully
- **Monitoring**: Provides visibility into processing status

### Configuration Flexibility

The framework uses environment variables for configuration:

- **Environment-Specific Settings**: Different settings for development, testing, and production
- **Sensitive Information**: Secure handling of credentials and tokens
- **Runtime Configuration**: Changes without code modification
- **Defaults**: Sensible defaults for optional settings

## Performance Considerations

1. **Batch Processing**: Data is processed in batches to optimize throughput
2. **Connection Pooling**: Database connections are reused for efficiency
3. **Bulk Operations**: Multiple records are processed in a single operation
4. **Indexing Strategy**: Carefully designed indexes improve query performance
5. **Partitioning**: Data is partitioned by date for efficient querying
6. **Compression**: Data is compressed to reduce storage and transfer costs
7. **Caching**: Frequently accessed data is cached for faster access
8. **Asynchronous Operations**: Non-blocking operations improve throughput

## Error Handling Strategy

1. **Retry Mechanism**: Automatic retries for transient failures
2. **Circuit Breaker**: Prevents cascading failures
3. **Graceful Degradation**: Continues operation with reduced functionality
4. **Comprehensive Logging**: Detailed logs for debugging
5. **Error Classification**: Different strategies for different error types
6. **Transaction Management**: Ensures data consistency
7. **Monitoring and Alerting**: Proactive notification of issues

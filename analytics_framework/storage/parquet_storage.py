"""Store and retrieve data in Parquet format on S3 or local filesystem."""

import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import (
    S3_ENABLED,
    S3_BUCKET,
    S3_PREFIX,
    PARQUET_STORAGE_ENABLED,
    PARQUET_BASE_DIR,
    PARQUET_PARTITION_BY,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_PAGE_SIZE,
    PARQUET_TARGET_FILE_SIZE_MB,
    PARQUET_MAX_RECORDS_PER_FILE
)


class ParquetStorage:
    """Store and retrieve data in Parquet format on S3 or local filesystem."""
    
    def __init__(
        self,
        use_s3: bool = S3_ENABLED,
        s3_bucket: Optional[str] = S3_BUCKET,
        s3_prefix: str = S3_PREFIX,
        base_dir: str = PARQUET_BASE_DIR,
        partition_by: List[str] = PARQUET_PARTITION_BY,
        compression: str = PARQUET_COMPRESSION,
        row_group_size: int = PARQUET_ROW_GROUP_SIZE,
        page_size: int = PARQUET_PAGE_SIZE,
        target_file_size_mb: int = PARQUET_TARGET_FILE_SIZE_MB,
        max_records_per_file: int = PARQUET_MAX_RECORDS_PER_FILE
    ):
        """
        Initialize the Parquet storage.
        
        Args:
            use_s3: Whether to use S3 for storage
            s3_bucket: S3 bucket name
            s3_prefix: Prefix for S3 keys
            base_dir: Base directory for local storage
            partition_by: List of fields to partition by
            compression: Compression algorithm
            row_group_size: Number of rows per row group
            page_size: Page size in bytes
            target_file_size_mb: Target file size in MB
            max_records_per_file: Maximum number of records per file
        """
        self.logger = logging.getLogger(__name__)
        self.use_s3 = use_s3
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.base_dir = base_dir
        self.partition_by = partition_by
        self.compression = compression
        self.row_group_size = row_group_size
        self.page_size = page_size
        self.target_file_size_mb = target_file_size_mb
        self.max_records_per_file = max_records_per_file
        
        # Initialize filesystem
        if self.use_s3:
            try:
                import s3fs
                self.fs = s3fs.S3FileSystem()
                self.logger.info(f"Using S3 for Parquet storage: s3://{self.s3_bucket}/{self.s3_prefix}")
            except ImportError:
                self.logger.warning("s3fs not available, falling back to local filesystem")
                self.use_s3 = False
                self.fs = None
        else:
            self.fs = None
            self.logger.info(f"Using local filesystem for Parquet storage: {self.base_dir}")
            
            # Create base directory if it doesn't exist
            os.makedirs(self.base_dir, exist_ok=True)
    
    def _get_path(self, data_type: str, partition_values: Optional[Dict[str, str]] = None) -> str:
        """
        Get the path for a data type and partition values.
        
        Args:
            data_type: Type of data (e.g., 'conversations', 'messages')
            partition_values: Dictionary of partition values
            
        Returns:
            Path string
        """
        if self.use_s3:
            base_path = f"s3://{self.s3_bucket}/{self.s3_prefix}/{data_type}"
        else:
            base_path = os.path.join(self.base_dir, data_type)
        
        if not partition_values:
            return base_path
        
        # Add partition directories without "key=" prefix
        partition_path = ""
        for key in self.partition_by:
            if key in partition_values:
                partition_path = os.path.join(partition_path, partition_values[key])
        
        return os.path.join(base_path, partition_path)
    
    def _get_path_prefix_for_file(self, data_type: str, partition_values: Optional[Dict[str, str]] = None) -> str:
        """
        Get the prefix for a file name based on partition values.
        
        Args:
            data_type: Type of data (e.g., 'conversations', 'messages')
            partition_values: Dictionary of partition values
            
        Returns:
            File prefix string
        """
        if not partition_values:
            return ""
        
        # Create a prefix for the filename using partition values
        prefix_parts = []
        for key in self.partition_by:
            if key in partition_values:
                prefix_parts.append(partition_values[key])
        
        if prefix_parts:
            return "_".join(prefix_parts) + "_"
        
        return ""
    
    def _extract_partition_values(self, record: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract partition values from a record.
        
        Args:
            record: Record to extract partition values from
            
        Returns:
            Dictionary of partition values
        """
        partition_values = {}
        
        # Extract date components from created_at
        created_at = record.get("created_at")
        if created_at and isinstance(created_at, str):
            try:
                dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                
                if "year" in self.partition_by:
                    partition_values["year"] = str(dt.year)
                
                if "month" in self.partition_by:
                    partition_values["month"] = f"{dt.month:02d}"
                
                if "day" in self.partition_by:
                    partition_values["day"] = f"{dt.day:02d}"
                
                if "hour" in self.partition_by:
                    partition_values["hour"] = f"{dt.hour:02d}"
            except ValueError:
                self.logger.warning(f"Invalid datetime format: {created_at}")
        
        # Extract other partition values
        for key in self.partition_by:
            if key not in partition_values and key in record:
                partition_values[key] = str(record[key])
        
        return partition_values
    
    def _records_to_dataframe(self, records: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convert records to a pandas DataFrame.
        
        Args:
            records: List of records
            
        Returns:
            Pandas DataFrame
        """
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Handle nested fields
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (dict, list)) else x)
        
        return df
    
    def store_conversations(self, conversations: List[Dict[str, Any]]) -> List[str]:
        """
        Store conversations in Parquet format.
        
        Args:
            conversations: List of conversation documents
            
        Returns:
            List of paths where the data was stored
        """
        if not PARQUET_STORAGE_ENABLED:
            self.logger.info("Parquet storage is disabled")
            return []
        
        if not conversations:
            return []
        
        self.logger.info(f"Storing {len(conversations)} conversations in Parquet format")
        
        # Group conversations by partition
        conversations_by_partition = {}
        
        for conversation in conversations:
            partition_values = self._extract_partition_values(conversation)
            partition_key = str(partition_values)
            
            if partition_key not in conversations_by_partition:
                conversations_by_partition[partition_key] = []
            
            conversations_by_partition[partition_key].append(conversation)
        
        # Store each partition
        stored_paths = []
        
        for partition_key, partition_conversations in conversations_by_partition.items():
            partition_values = eval(partition_key)
            
            # Convert to DataFrame
            df = self._records_to_dataframe(partition_conversations)
            
            # Get path
            path = self._get_path("conversations", partition_values)
            
            # Get file prefix
            file_prefix = self._get_path_prefix_for_file("conversations", partition_values)
            
            # Store DataFrame
            self._store_dataframe(df, path, f"{file_prefix}conversations.parquet")
            stored_paths.append(path)
            
            # Extract and store messages
            messages = []
            for conversation in partition_conversations:
                conversation_id = conversation.get("_id")
                conversation_messages = conversation.get("messages", [])
                
                for message in conversation_messages:
                    message_copy = message.copy()
                    message_copy["conversation_id"] = conversation_id
                    messages.append(message_copy)
            
            if messages:
                messages_df = self._records_to_dataframe(messages)
                messages_path = os.path.join(path, "messages")
                self._store_dataframe(messages_df, messages_path, f"{file_prefix}messages.parquet")
                stored_paths.append(messages_path)
            
            # Extract and store categories
            categories = []
            for conversation in partition_conversations:
                conversation_id = conversation.get("_id")
                conversation_categories = conversation.get("categories", [])
                
                for category in conversation_categories:
                    category_copy = category.copy()
                    category_copy["conversation_id"] = conversation_id
                    categories.append(category_copy)
            
            if categories:
                categories_df = self._records_to_dataframe(categories)
                categories_path = os.path.join(path, "categories")
                self._store_dataframe(categories_df, categories_path, f"{file_prefix}categories.parquet")
                stored_paths.append(categories_path)
        
        return stored_paths
    
    def store_user_analytics(self, user_analytics: List[Dict[str, Any]]) -> str:
        """
        Store user analytics in Parquet format.
        
        Args:
            user_analytics: List of user analytics documents
            
        Returns:
            Path where the data was stored
        """
        if not PARQUET_STORAGE_ENABLED:
            self.logger.info("Parquet storage is disabled")
            return ""
        
        if not user_analytics:
            return ""
        
        self.logger.info(f"Storing {len(user_analytics)} user analytics records in Parquet format")
        
        # Convert to DataFrame
        df = self._records_to_dataframe(user_analytics)
        
        # Get path
        path = self._get_path("user_analytics")
        
        # Store DataFrame
        self._store_dataframe(df, path, "user_analytics.parquet")
        
        return path
    
    def store_analytics_reports(self, reports: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Store analytics reports in Parquet format.
        
        Args:
            reports: List of analytics report documents
            
        Returns:
            Dictionary mapping report types to paths
        """
        if not PARQUET_STORAGE_ENABLED:
            self.logger.info("Parquet storage is disabled")
            return {}
        
        if not reports:
            return {}
        
        self.logger.info(f"Storing {len(reports)} analytics reports in Parquet format")
        
        # Group reports by type
        reports_by_type = {}
        
        for report in reports:
            report_type = report.get("report_type")
            if not report_type:
                continue
                
            if report_type not in reports_by_type:
                reports_by_type[report_type] = []
                
            reports_by_type[report_type].append(report)
        
        # Store each report type
        stored_paths = {}
        
        for report_type, type_reports in reports_by_type.items():
            # Convert to DataFrame
            df = self._records_to_dataframe(type_reports)
            
            # Get path
            path = self._get_path(f"analytics_reports/{report_type}")
            
            # Store DataFrame
            self._store_dataframe(df, path, f"{report_type}_reports.parquet")
            stored_paths[report_type] = path
        
        return stored_paths
    
    def _store_dataframe(self, df: pd.DataFrame, path: str, filename: str) -> None:
        """
        Store a DataFrame in Parquet format.
        
        Args:
            df: DataFrame to store
            path: Path to store the DataFrame
            filename: Filename for the Parquet file
        """
        # Create directory if it doesn't exist
        if not self.use_s3:
            os.makedirs(path, exist_ok=True)
        
        # Full path to the Parquet file
        full_path = os.path.join(path, filename)
        
        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet
        try:
            if self.use_s3:
                # For S3, we use pyarrow's write_to_dataset
                # Modified to include {i} in basename_template for partitioning
                base_filename = os.path.splitext(filename)[0]
                extension = os.path.splitext(filename)[1]
                basename_template = f"{base_filename}_{{i}}{extension}"
                
                pq.write_to_dataset(
                    table,
                    root_path=path,
                    filesystem=self.fs,
                    basename_template=basename_template,
                    compression=self.compression,
                    row_group_size=self.row_group_size,
                    data_page_size=self.page_size,
                    use_dictionary=True,
                    write_statistics=True
                )
            else:
                # For local filesystem, we use pyarrow's write_table
                pq.write_table(
                    table,
                    full_path,
                    compression=self.compression,
                    row_group_size=self.row_group_size,
                    data_page_size=self.page_size,
                    use_dictionary=True,
                    write_statistics=True
                )
            
            self.logger.info(f"Stored {len(df)} records in {full_path}")
        except Exception as e:
            self.logger.error(f"Error storing DataFrame in {full_path}: {str(e)}")
            raise
    
    def read_conversations(
        self,
        partition_values: Optional[Dict[str, str]] = None,
        filters: Optional[List[tuple]] = None
    ) -> pd.DataFrame:
        """
        Read conversations from Parquet format.
        
        Args:
            partition_values: Dictionary of partition values
            filters: List of filter tuples (column, op, value)
            
        Returns:
            DataFrame of conversations
        """
        if not PARQUET_STORAGE_ENABLED:
            self.logger.info("Parquet storage is disabled")
            return pd.DataFrame()
        
        # Get path
        path = self._get_path("conversations", partition_values)
        
        # Get file prefix
        file_prefix = self._get_path_prefix_for_file("conversations", partition_values)
        
        # Read DataFrame
        return self._read_dataframe(path, f"{file_prefix}conversations.parquet", filters)
    
    def read_messages(
        self,
        partition_values: Optional[Dict[str, str]] = None,
        filters: Optional[List[tuple]] = None
    ) -> pd.DataFrame:
        """
        Read messages from Parquet format.
        
        Args:
            partition_values: Dictionary of partition values
            filters: List of filter tuples (column, op, value)
            
        Returns:
            DataFrame of messages
        """
        if not PARQUET_STORAGE_ENABLED:
            self.logger.info("Parquet storage is disabled")
            return pd.DataFrame()
        
        # Get path
        path = self._get_path("conversations", partition_values)
        messages_path = os.path.join(path, "messages")
        
        # Get file prefix
        file_prefix = self._get_path_prefix_for_file("conversations", partition_values)
        
        # Read DataFrame
        return self._read_dataframe(messages_path, f"{file_prefix}messages.parquet", filters)
    
    def read_categories(
        self,
        partition_values: Optional[Dict[str, str]] = None,
        filters: Optional[List[tuple]] = None
    ) -> pd.DataFrame:
        """
        Read categories from Parquet format.
        
        Args:
            partition_values: Dictionary of partition values
            filters: List of filter tuples (column, op, value)
            
        Returns:
            DataFrame of categories
        """
        if not PARQUET_STORAGE_ENABLED:
            self.logger.info("Parquet storage is disabled")
            return pd.DataFrame()
        
        # Get path
        path = self._get_path("conversations", partition_values)
        categories_path = os.path.join(path, "categories")
        
        # Get file prefix
        file_prefix = self._get_path_prefix_for_file("conversations", partition_values)
        
        # Read DataFrame
        return self._read_dataframe(categories_path, f"{file_prefix}categories.parquet", filters)
    
    def read_user_analytics(
        self,
        filters: Optional[List[tuple]] = None
    ) -> pd.DataFrame:
        """
        Read user analytics from Parquet format.
        
        Args:
            filters: List of filter tuples (column, op, value)
            
        Returns:
            DataFrame of user analytics
        """
        if not PARQUET_STORAGE_ENABLED:
            self.logger.info("Parquet storage is disabled")
            return pd.DataFrame()
        
        # Get path
        path = self._get_path("user_analytics")
        
        # Read DataFrame
        return self._read_dataframe(path, "user_analytics.parquet", filters)
    
    def read_analytics_reports(
        self,
        report_type: str,
        filters: Optional[List[tuple]] = None
    ) -> pd.DataFrame:
        """
        Read analytics reports from Parquet format.
        
        Args:
            report_type: Type of report
            filters: List of filter tuples (column, op, value)
            
        Returns:
            DataFrame of analytics reports
        """
        if not PARQUET_STORAGE_ENABLED:
            self.logger.info("Parquet storage is disabled")
            return pd.DataFrame()
        
        # Get path
        path = self._get_path(f"analytics_reports/{report_type}")
        
        # Read DataFrame
        return self._read_dataframe(path, f"{report_type}_reports.parquet", filters)
    
    def _read_dataframe(
        self,
        path: str,
        filename: str,
        filters: Optional[List[tuple]] = None
    ) -> pd.DataFrame:
        """
        Read a DataFrame from Parquet format.
        
        Args:
            path: Path to read the DataFrame from
            filename: Filename for the Parquet file
            filters: List of filter tuples (column, op, value)
            
        Returns:
            DataFrame
        """
        # Full path to the Parquet file
        full_path = os.path.join(path, filename)
        
        try:
            if self.use_s3:
                # For S3, we use pyarrow's read_table with filesystem
                table = pq.read_table(
                    full_path,
                    filesystem=self.fs,
                    filters=filters
                )
            else:
                # Check if file exists
                if not os.path.exists(full_path):
                    self.logger.warning(f"File not found: {full_path}")
                    return pd.DataFrame()
                
                # For local filesystem, we use pyarrow's read_table
                table = pq.read_table(
                    full_path,
                    filters=filters
                )
            
            # Convert to DataFrame
            df = table.to_pandas()
            
            self.logger.info(f"Read {len(df)} records from {full_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error reading DataFrame from {full_path}: {str(e)}")
            return pd.DataFrame()
    
    def list_partitions(self, data_type: str) -> List[Dict[str, str]]:
        """
        List available partitions for a data type.
        
        Args:
            data_type: Type of data
            
        Returns:
            List of partition value dictionaries
        """
        if not PARQUET_STORAGE_ENABLED:
            self.logger.info("Parquet storage is disabled")
            return []
        
        # Get base path
        base_path = self._get_path(data_type)
        
        try:
            partitions = []
            partition_keys = self.partition_by.copy()
            
            if self.use_s3:
                # List directories in S3
                paths = self.fs.glob(f"{base_path}/**/*.parquet")
                
                for path in paths:
                    # Extract partition values from path structure
                    relative_path = path.replace(base_path, "").strip("/").split("/")
                    if len(relative_path) >= len(partition_keys):
                        partition_values = {}
                        # Assuming directory structure matches partition keys order
                        for i, key in enumerate(partition_keys):
                            if i < len(relative_path) - 1:  # Skip the filename
                                partition_values[key] = relative_path[i]
                        
                        if partition_values and partition_values not in partitions:
                            partitions.append(partition_values)
            else:
                # List directories in local filesystem
                for root, _, files in os.walk(base_path):
                    if any(f.endswith(".parquet") for f in files):
                        # Extract partition values from path structure
                        relative_path = os.path.relpath(root, base_path).split(os.path.sep)
                        if len(relative_path) >= len(partition_keys):
                            partition_values = {}
                            # Assuming directory structure matches partition keys order
                            for i, key in enumerate(partition_keys):
                                if i < len(relative_path):
                                    partition_values[key] = relative_path[i]
                            
                            if partition_values and partition_values not in partitions:
                                partitions.append(partition_values)
            
            return partitions
        except Exception as e:
            self.logger.error(f"Error listing partitions for {data_type}: {str(e)}")
            return []

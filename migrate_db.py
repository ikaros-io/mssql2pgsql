import argparse
import json
import logging
import os
import pickle
import re
import sys
import threading
import time
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict
from typing import Optional

import psycopg2
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import DECIMAL
from sqlalchemy import Float
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.sql import select
from sqlalchemy.types import TypeDecorator
from sqlalchemy.types import VARCHAR


class CleanLogger:
    """
    Clean logging system inspired by dbt's approach for multi-threaded
    operations.
    """

    def __init__(self, log_level=logging.INFO):
        self.logger = logging.getLogger('migration')
        self.logger.setLevel(log_level)

        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Create console handler with clean formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)

        # Create formatter inspired by dbt's style
        formatter = logging.Formatter(
            '%(asctime)s  [%(levelname)s] %(thread_name)s: %(message)s',
            datefmt='%H:%M:%S',
        )
        console_handler.setFormatter(formatter)

        self.logger.addHandler(console_handler)

        # Thread-safe lock for consistent output
        self._lock = threading.Lock()

    def _get_thread_name(self):
        """
        Get a clean thread name for logging.
        """
        thread = threading.current_thread()
        if thread.name.startswith('ThreadPoolExecutor'):
            # Extract worker number from thread name
            parts = thread.name.split('-')
            if len(parts) >= 2:
                return f"Worker-{parts[-1]}"
        return thread.name

    def info(self, message, **kwargs):
        """
        Log info message with thread context.
        """
        with self._lock:
            extra = {'thread_name': self._get_thread_name()}
            self.logger.info(message, extra=extra, **kwargs)

    def warning(self, message, **kwargs):
        """
        Log warning message with thread context.
        """
        with self._lock:
            extra = {'thread_name': self._get_thread_name()}
            self.logger.warning(message, extra=extra, **kwargs)

    def error(self, message, **kwargs):
        """
        Log error message with thread context.
        """
        with self._lock:
            extra = {'thread_name': self._get_thread_name()}
            self.logger.error(message, extra=extra, **kwargs)

    def success(self, message, **kwargs):
        """
        Log success message (info level with ✓ prefix).
        """
        self.info(f"✓ {message}", **kwargs)

    def failure(self, message, **kwargs):
        """
        Log failure message (error level with ✗ prefix).
        """
        self.error(f"✗ {message}", **kwargs)

    def skip(self, message, **kwargs):
        """
        Log skip message (info level with ⏭ prefix).
        """
        self.info(f"⏭ {message}", **kwargs)

    def progress(self, message, **kwargs):
        """
        Log progress message (info level with → prefix).
        """
        self.info(f"→ {message}", **kwargs)


# Global logger instance
logger = CleanLogger()


@dataclass
class TableBatchConfig:
    """
    Configuration for table batching based on previous runs.
    """

    table_name: str
    column_count: int
    avg_row_size: Optional[int] = None
    optimal_chunk_size: int = 10000
    optimal_sub_batch_size: int = 1000
    success_rate: float = 1.0
    last_run_time: Optional[float] = None
    geo_columns: int = 0
    large_text_columns: int = 0
    total_rows: int = 0
    processed_rows: int = 0  # Track progress within a table
    last_successful_offset: int = 0  # Last successfully processed offset
    migration_status: str = "pending"  # pending, in_progress, completed, failed
    temp_table_name: str = ""  # Name of the temp table if it exists
    temp_table_exists: bool = False  # Track if temp table exists
    notes: str = ""

    def calculate_optimal_sizes(self):
        """
        Calculate optimal batch sizes based on table characteristics.
        """
        base_chunk = 10000
        base_sub_batch = 1000

        # Adjust for column count
        if self.column_count > 100:
            base_chunk = 2000
            base_sub_batch = 200
        elif self.column_count > 50:
            base_chunk = 5000
            base_sub_batch = 400
        elif self.column_count > 30:
            base_chunk = 8000
            base_sub_batch = 600

        # Adjust for geo columns
        if self.geo_columns > 0:
            base_chunk = int(base_chunk * 0.7)
            base_sub_batch = int(base_sub_batch * 0.7)

        # Adjust for large text columns
        if self.large_text_columns > 0:
            base_chunk = int(base_chunk * 0.8)
            base_sub_batch = int(base_sub_batch * 0.8)

        # Adjust based on previous success rate
        if self.success_rate < 0.8:
            base_chunk = int(base_chunk * 0.5)
            base_sub_batch = int(base_sub_batch * 0.5)

        self.optimal_chunk_size = max(1000, base_chunk)
        self.optimal_sub_batch_size = max(100, base_sub_batch)


class BatchConfigManager:
    """
    Manages batch configuration persistence using pickle files.
    """

    def __init__(self, config_file: str = "batch_config.pkl"):
        self.config_file = Path(config_file)
        self.configs: Dict[str, TableBatchConfig] = {}
        self.load_config()

    def load_config(self):
        """
        Load batch configuration from pickle file.
        """
        if self.config_file.exists():
            try:
                with open(self.config_file, 'rb') as f:
                    self.configs = pickle.load(f)
                logger.info(
                    f"Loaded batch configuration for {len(self.configs)} tables"
                )

                # Log some details about loaded configs
                for table_name, config in self.configs.items():
                    logger.info(
                        f"  {table_name}: chunk={config.optimal_chunk_size}, sub_batch={config.optimal_sub_batch_size}, success={config.success_rate:.2%}"
                    )

            except Exception as e:
                logger.warning(f"Failed to load batch config: {e}")
                self.configs = {}
        else:
            logger.info(
                "No existing batch configuration found, will create new one"
            )

    def save_config(self):
        """
        Save batch configuration to pickle file.
        """
        try:
            with open(self.config_file, 'wb') as f:
                pickle.dump(self.configs, f)
            logger.success(
                f"Saved batch configuration for {len(self.configs)} tables to {self.config_file}"
            )

            # Also save a human-readable JSON version for inspection
            json_file = self.config_file.with_suffix('.json')
            with open(json_file, 'w') as f:
                json_data = {
                    name: asdict(config)
                    for name, config in self.configs.items()
                }
                json.dump(json_data, f, indent=2, default=str)
            logger.info(f"Also saved readable version to {json_file}")

        except Exception as e:
            logger.error(f"Failed to save batch config: {e}")

    def get_config(self, table_name: str, columns: list) -> TableBatchConfig:
        """
        Get or create batch configuration for a table.
        """
        if table_name in self.configs:
            config = self.configs[table_name]
            # Update column count in case table structure changed
            config.column_count = len(columns)

            # Check if we can resume from a partial migration
            if (
                config.migration_status == "partial"
                and config.last_successful_offset > 0
            ):
                logger.info(
                    f"Found partial migration for {table_name}: can resume from row {config.last_successful_offset:,}"
                )
                logger.info(
                    f"  Previous progress: {config.processed_rows:,}/{config.total_rows:,} ({config.processed_rows/config.total_rows*100:.1f}%)"
                )
            elif config.migration_status == "completed":
                logger.info(
                    f"Table {table_name} was previously completed successfully"
                )

            logger.info(
                f"Using existing config for {table_name}: chunk={config.optimal_chunk_size}, sub_batch={config.optimal_sub_batch_size}"
            )
        else:
            # Analyze columns for new table
            geo_columns = sum(
                1
                for col in columns
                if col['type'].lower() in ['geography', 'geometry']
            )
            large_text_columns = sum(
                1
                for col in columns
                if col['type'].lower() in ['text', 'ntext']
                or (
                    col['type'].lower() in ['varchar', 'nvarchar']
                    and col.get('max_length', 0) > 1000
                )
            )

            config = TableBatchConfig(
                table_name=table_name,
                column_count=len(columns),
                geo_columns=geo_columns,
                large_text_columns=large_text_columns,
            )
            config.calculate_optimal_sizes()
            self.configs[table_name] = config
            logger.info(
                f"Created new config for {table_name}: chunk={config.optimal_chunk_size}, sub_batch={config.optimal_sub_batch_size}"
            )
            logger.info(
                f"  Analysis: {len(columns)} columns, {geo_columns} geo, {large_text_columns} large text"
            )

        return config

    def update_config(
        self,
        table_name: str,
        success_rate: float,
        processing_time: float,
        total_rows: int,
        processed_rows: int = 0,
        notes: str = "",
    ):
        """
        Update configuration based on migration results.
        """
        if table_name in self.configs:
            config = self.configs[table_name]
            config.success_rate = success_rate
            config.last_run_time = processing_time
            config.total_rows = total_rows
            config.processed_rows = processed_rows
            config.notes = notes

            # Update migration status
            if success_rate >= 1.0:
                config.migration_status = "completed"
                config.last_successful_offset = total_rows
            elif success_rate > 0:
                config.migration_status = "partial"
                config.last_successful_offset = processed_rows
            else:
                config.migration_status = "failed"

            # Adjust batch sizes based on success rate
            if success_rate < 0.8:
                # Migration had issues, reduce batch sizes
                config.optimal_chunk_size = int(config.optimal_chunk_size * 0.7)
                config.optimal_sub_batch_size = int(
                    config.optimal_sub_batch_size * 0.7
                )
                logger.info(
                    f"Reduced batch sizes for {table_name} due to low success rate ({success_rate:.2%})"
                )
            elif success_rate > 0.95 and processing_time > 0:
                # Migration was very successful, we can try larger batches next time
                rows_per_second = (
                    total_rows / processing_time if processing_time > 0 else 0
                )
                if rows_per_second > 1000:  # Good performance
                    config.optimal_chunk_size = int(
                        config.optimal_chunk_size * 1.2
                    )
                    config.optimal_sub_batch_size = int(
                        config.optimal_sub_batch_size * 1.1
                    )
                    logger.info(
                        f"Increased batch sizes for {table_name} due to good performance ({rows_per_second:.0f} rows/sec)"
                    )

            # Keep batch sizes within reasonable bounds
            config.optimal_chunk_size = max(
                1000, min(50000, config.optimal_chunk_size)
            )
            config.optimal_sub_batch_size = max(
                100, min(2000, config.optimal_sub_batch_size)
            )

            logger.info(
                f"Updated config for {table_name}: success={success_rate:.2%}, time={processing_time:.1f}s, processed={processed_rows}/{total_rows}, new_chunk={config.optimal_chunk_size}"
            )


# Global batch config manager (will be initialized in main())
batch_manager = None


class GeographyType(TypeDecorator):
    """
    Custom type to handle SQL Server geography/geometry types.

    Converts to TEXT in PostgreSQL.
    """

    impl = VARCHAR
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        # Convert geography/geometry to WKT string
        return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return str(value)


def map_sql_server_type_to_postgres(sql_type, column_name=None):
    """
    Map SQL Server data types to PostgreSQL SQLAlchemy types.

    Handles special cases like geography and geometry types.
    """
    if sql_type is None:
        logger.warning(
            f"NULL type for column '{column_name}', defaulting to TEXT"
        )
        return Text

    sql_type_lower = str(sql_type).lower()

    # Handle geography and geometry types - convert to TEXT since we're using STAsText()
    if sql_type_lower in ['geography', 'geometry']:
        logger.info(
            f"Mapping geo type '{sql_type}' to TEXT for column '{column_name}'"
        )
        return Text

    # Handle other common type mappings
    type_mapping = {
        'varchar': String,
        'nvarchar': String,
        'char': String,
        'nchar': String,
        'text': Text,
        'ntext': Text,
        'int': BigInteger,  # Use BigInteger for SQL Server int to handle large values
        'bigint': BigInteger,
        'smallint': Integer,
        'tinyint': Integer,
        'bit': Boolean,
        'float': Float,
        'real': Float,
        'decimal': DECIMAL,
        'numeric': DECIMAL,
        'money': DECIMAL,
        'smallmoney': DECIMAL,
        'datetime': DateTime,
        'datetime2': DateTime,
        'smalldatetime': DateTime,
        'date': DateTime,
        'time': DateTime,
        'uniqueidentifier': UUID,
    }

    for sql_key, pg_type in type_mapping.items():
        if sql_key in sql_type_lower:
            return pg_type

    # Default to Text for unknown types
    logger.warning(
        f"Unknown type '{sql_type}' for column '{column_name}', defaulting to TEXT"
    )
    return Text


def get_connection_string(config):
    """
    Build SQL Server connection string.
    """
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},1433;"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        "Trusted_Connection=no;"
        "TrustServerCertificate=yes;"
        "LoginTimeout=30;"
        "Timeout=30;"
        "ConnectRetryCount=3;"
        "ConnectRetryInterval=10;"
    )


def get_postgres_connection_string(config):
    """
    Build PostgreSQL connection string.
    """
    return f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"


def create_postgres_schema(pg_config):
    """
    Create the target schema in PostgreSQL if it doesn't exist.
    """
    try:
        conn_str = get_postgres_connection_string(pg_config)
        engine = create_engine(conn_str)

        with engine.connect() as conn:
            conn.execute(
                text(f"CREATE SCHEMA IF NOT EXISTS {pg_config['schema']}")
            )
            conn.commit()

        logger.success(
            f"Schema '{pg_config['schema']}' created/verified in PostgreSQL"
        )
        return True
    except Exception as e:
        logger.failure(f"Failed to create schema: {str(e)}")
        return False


def get_schema_tables(cursor, schema_name):
    """
    Get all table names from the specified schema.
    """
    query = """
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = ? 
    AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
    """

    cursor.execute(query, schema_name)
    tables = [row[0] for row in cursor.fetchall()]
    return tables


def get_table_info_direct(engine, schema_name, table_name):
    """
    Get table information using direct SQL queries to handle geography types.

    Returns row count and column information.
    """
    try:
        with engine.connect() as conn:
            # Get row count
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]")
            )
            row_count = result.scalar()

            # Get column information using INFORMATION_SCHEMA
            columns_query = text(
                """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = :schema_name 
                AND TABLE_NAME = :table_name
                ORDER BY ORDINAL_POSITION
            """
            )

            result = conn.execute(
                columns_query,
                {"schema_name": schema_name, "table_name": table_name},
            )
            columns = []

            for row in result:
                col_info = {
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'max_length': row[3],
                    'precision': row[4],
                    'scale': row[5],
                }
                columns.append(col_info)

                # Log geography types for debugging
                if row[1].lower() in ['geography', 'geometry']:
                    logger.info(
                        f"Found geo column '{row[0]}' of type '{row[1]}'"
                    )

            return row_count, columns

    except Exception as e:
        logger.error(f"Failed to get table info for {table_name}: {e}")
        return 0, []


def create_postgres_table_sqlalchemy(pg_engine, pg_schema, table_name, columns):
    """
    Create PostgreSQL table using SQLAlchemy with proper type mapping.
    """
    metadata = MetaData()

    # Build columns for the new table
    pg_columns = []
    for col in columns:
        col_name = col['name']
        col_type = col.get('type', 'varchar')
        nullable = col.get('nullable', True)

        # Map SQL Server type to PostgreSQL type
        pg_type = map_sql_server_type_to_postgres(str(col_type), col_name)

        # Handle string lengths for varchar/nvarchar
        if col_type.lower() in [
            'varchar',
            'nvarchar',
            'char',
            'nchar',
        ] and col.get('max_length'):
            max_length = col['max_length']
            if (
                max_length > 0 and max_length <= 10485760
            ):  # PostgreSQL varchar limit
                pg_type = String(max_length)
            else:
                pg_type = Text  # Use Text for very long or unlimited strings

        # Handle decimal precision/scale
        elif col_type.lower() in ['decimal', 'numeric'] and col.get(
            'precision'
        ):
            precision = col.get('precision')
            scale = col.get('scale', 0)
            if precision and precision > 0:
                pg_type = DECIMAL(precision, scale)

        pg_columns.append(Column(col_name, pg_type, nullable=nullable))

        # Log problematic column types
        if col_type.lower() in ['int', 'bigint']:
            logger.info(
                f"Column {col_name} ({col_type}) mapped to BigInteger for large value support"
            )

    # Create table object
    table = Table(table_name, metadata, *pg_columns, schema=pg_schema)

    # Create the table
    try:
        metadata.create_all(pg_engine, tables=[table])
        logger.success(f"Created table structure for {table_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        return False


def table_exists_in_postgres(pg_config, table_name):
    """
    Check if table already exists in PostgreSQL.
    """
    try:
        conn_str = get_postgres_connection_string(pg_config)
        engine = create_engine(conn_str)

        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = :schema 
                    AND table_name = :table_name
                )
            """
                ),
                {"schema": pg_config['schema'], "table_name": table_name},
            )

            return result.scalar()
    except Exception:
        return False


def get_temp_table_row_count(pg_config, temp_table_name):
    """
    Get row count from a temp table to validate partial migration.
    """
    try:
        conn_str = get_postgres_connection_string(pg_config)
        engine = create_engine(conn_str)

        with engine.connect() as conn:
            result = conn.execute(
                text(
                    f'SELECT COUNT(*) FROM {pg_config["schema"]}."{temp_table_name}"'
                )
            )
            return result.scalar()
    except Exception as e:
        logger.warning(f"Failed to get temp table row count: {e}")
        return 0


def get_existing_postgres_tables(pg_config):
    """
    Get all existing table names from PostgreSQL schema.
    """
    try:
        conn_str = get_postgres_connection_string(pg_config)
        engine = create_engine(conn_str)

        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = :schema 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
                ),
                {"schema": pg_config['schema']},
            )

            return [row[0] for row in result.fetchall()]
    except Exception as e:
        logger.failure(f"Failed to get existing PostgreSQL tables: {str(e)}")
        return []


def migrate_table_sqlalchemy(
    config, schema_name, table_name, pg_config, chunk_size=None
):
    """
    Migrate a single table from SQL Server to PostgreSQL using pure SQLAlchemy.

    Handles geo types, null values, and provides better error handling.
    """
    start_time = time.time()
    temp_table = None

    try:
        # Check if table already exists in PostgreSQL
        if table_exists_in_postgres(pg_config, table_name):
            logger.skip(
                f"Skipped {table_name}: Table already exists in PostgreSQL"
            )
            return True, table_name

        # Create SQL Server engine
        port = config.get('port', 1433)
        driver_param = (
            config['driver']
            .replace(' ', '+')
            .replace('SQL+Server', 'SQL+Server')
        )
        timeout = config.get('connection', {}).get('timeout', 300)
        trust_cert = (
            'yes'
            if config.get('connection', {}).get(
                'trust_server_certificate', True
            )
            else 'no'
        )

        sql_server_conn_str = f"mssql+pyodbc://{config['username']}:{config['password']}@{config['server']},{port}/{config['database']}?driver={driver_param}&TrustServerCertificate={trust_cert}&timeout={timeout}"
        ss_engine = create_engine(
            sql_server_conn_str, pool_timeout=timeout, pool_recycle=3600
        )

        # Create PostgreSQL engine
        pg_conn_str = get_postgres_connection_string(pg_config)
        pg_engine = create_engine(pg_conn_str)

        # Get table info, row count, and primary key information
        total_rows, columns, primary_keys = get_table_info_direct(
            ss_engine, schema_name, table_name
        )

        # Get optimal batch configuration
        table_config = batch_manager.get_config(table_name, columns)
        if chunk_size is None:
            chunk_size = table_config.optimal_chunk_size

        logger.progress(
            f"Migrating {table_name} to PostgreSQL: {total_rows:,} rows in chunks of {chunk_size:,}"
        )
        logger.progress(f"→ Source: {schema_name}.{table_name}")
        logger.progress(f"→ Columns: {len(columns)}")
        logger.progress(
            f"→ Using optimized batch config: chunk={chunk_size}, sub_batch={table_config.optimal_sub_batch_size}"
        )

        if total_rows == 0:
            # Create empty table with proper structure
            if create_postgres_table_sqlalchemy(
                pg_engine, pg_config['schema'], table_name, columns
            ):
                logger.success(f"Created empty table {table_name}: 0 rows")
                return True, table_name
            else:
                return False, table_name

        # Create or reuse temporary table
        if not use_existing_temp_table:
            temp_table = f"{table_name}_temp_{int(time.time() * 1000)}"
            logger.progress(f"Creating new temporary table: {temp_table}")

            # Create temp table structure first
            if not create_postgres_table_sqlalchemy(
                pg_engine, pg_config['schema'], temp_table, columns
            ):
                return False, table_name

            # Update config with new temp table info
            table_config.temp_table_name = temp_table
            table_config.temp_table_exists = True
        else:
            logger.progress(f"Reusing existing temporary table: {temp_table}")

        # Check if we can resume from a previous partial migration
        resume_offset = 0
        use_existing_temp_table = False

        if (
            table_config.migration_status == "partial"
            and table_config.last_successful_offset > 0
            and table_config.temp_table_exists
            and table_config.temp_table_name
        ):

            # Validate that the temp table still exists and has the expected data
            if table_exists_in_postgres(
                pg_config, table_config.temp_table_name
            ):
                temp_row_count = get_temp_table_row_count(
                    pg_config, table_config.temp_table_name
                )
                expected_rows = table_config.last_successful_offset

                if temp_row_count >= expected_rows * 0.95:  # Allow 5% tolerance
                    resume_offset = table_config.last_successful_offset
                    temp_table = table_config.temp_table_name
                    use_existing_temp_table = True
                    logger.info(
                        f"Found existing temp table with {temp_row_count:,} rows"
                    )
                    logger.info(
                        f"Resuming migration from offset {resume_offset:,} (skipping {resume_offset/total_rows*100:.1f}% already completed)"
                    )
                else:
                    logger.warning(
                        f"Temp table {table_config.temp_table_name} has {temp_row_count:,} rows, expected ~{expected_rows:,}"
                    )
                    logger.warning(
                        "Temp table data doesn't match expected progress, starting fresh"
                    )
                    # Clean up inconsistent temp table
                    cleanup_temp_table(
                        pg_config,
                        table_config.temp_table_name,
                        preserve_partial=False,
                    )
                    table_config.temp_table_exists = False
                    table_config.temp_table_name = ""
            else:
                logger.warning(
                    f"Expected temp table {table_config.temp_table_name} not found, starting fresh"
                )
                table_config.temp_table_exists = False
                table_config.temp_table_name = ""

        # Mark migration as in progress
        table_config.migration_status = "in_progress"
        batch_manager.save_config()  # Save progress state

        # Process data in chunks using SQLAlchemy
        total_processed = resume_offset  # Start from where we left off
        last_log_time = start_time
        last_progress_pct = (
            (resume_offset / total_rows * 100) if total_rows > 0 else 0
        )
        offset = resume_offset

        try:
            while total_processed < total_rows:
                # Build query with proper null handling and geo type conversion
                column_selects = []
                geo_columns = set()

                for col in columns:
                    col_name = col['name']
                    col_type = col['type'].lower()

                    # Handle geo types - convert to string representation
                    if col_type in ['geography', 'geometry']:
                        geo_columns.add(col_name)
                        # Use STAsText() to convert geography/geometry to WKT string, handle nulls
                        column_selects.append(
                            f"CASE WHEN [{col_name}] IS NULL THEN NULL ELSE CAST([{col_name}].STAsText() AS NVARCHAR(MAX)) END as [{col_name}]"
                        )
                        logger.info(
                            f"Converting geo column '{col_name}' using STAsText()"
                        )
                    else:
                        column_selects.append(f"[{col_name}]")

                # Generate deterministic ORDER BY clause
                if primary_keys:
                    # Use primary key columns for ordering
                    pk_columns = [f"[{pk}]" for pk in primary_keys]
                    order_clause = f"ORDER BY {', '.join(pk_columns)}"
                    if offset == resume_offset:  # Log only once
                        logger.info(f"Using primary key ordering: {', '.join(primary_keys)}")
                else:
                    # Fall back to physloc for tables without primary keys
                    order_clause = "ORDER BY %%physloc%%"
                    if offset == resume_offset:  # Log only once
                        logger.info("No primary key found, using %%physloc%% for deterministic ordering")

                query = f"""
                SELECT {', '.join(column_selects)}
                FROM [{schema_name}].[{table_name}]
                {order_clause}
                OFFSET {offset} ROWS
                FETCH NEXT {chunk_size} ROWS ONLY
                """

                logger.info(
                    f"Executing query with {len(geo_columns)} geo columns: {geo_columns}"
                )

                # Execute query and fetch results
                with ss_engine.connect() as ss_conn:
                    result = ss_conn.execute(text(query))
                    rows = result.fetchall()

                    if not rows:
                        break

                    # Convert rows to dictionaries with proper null handling
                    batch_data = []
                    column_names = [col['name'] for col in columns]

                    for row in rows:
                        row_dict = {}
                        for i, col_name in enumerate(column_names):
                            value = row[i] if i < len(row) else None
                            col_type = (
                                columns[i]['type'].lower()
                                if i < len(columns)
                                else 'varchar'
                            )

                            # Handle None/NULL values properly
                            if value is None:
                                row_dict[col_name] = None
                            else:
                                # Data type conversion and validation
                                try:
                                    if col_type in [
                                        'int',
                                        'bigint',
                                        'smallint',
                                        'tinyint',
                                    ]:
                                        # Handle large integers that might overflow PostgreSQL int
                                        if isinstance(value, (int, Decimal)):
                                            int_val = int(value)
                                            # PostgreSQL bigint range: -2^63 to 2^63-1
                                            if (
                                                int_val < -9223372036854775808
                                                or int_val > 9223372036854775807
                                            ):
                                                logger.warning(
                                                    f"Integer {int_val} out of bigint range for {col_name}, converting to string"
                                                )
                                                row_dict[col_name] = str(
                                                    int_val
                                                )
                                            else:
                                                row_dict[col_name] = int_val
                                        else:
                                            row_dict[col_name] = int(value)
                                    elif col_type in [
                                        'decimal',
                                        'numeric',
                                        'money',
                                        'smallmoney',
                                    ]:
                                        if isinstance(value, Decimal):
                                            row_dict[col_name] = value
                                        else:
                                            row_dict[col_name] = Decimal(
                                                str(value)
                                            )
                                    elif col_type == 'bit':
                                        row_dict[col_name] = bool(value)
                                    elif isinstance(value, bytes):
                                        try:
                                            row_dict[col_name] = value.decode(
                                                'utf-8'
                                            )
                                        except UnicodeDecodeError:
                                            row_dict[col_name] = str(value)
                                    else:
                                        row_dict[col_name] = value
                                except (
                                    ValueError,
                                    TypeError,
                                    OverflowError,
                                ) as e:
                                    logger.warning(
                                        f"Data conversion error for {col_name} (type: {col_type}, value: {value}): {e}"
                                    )
                                    # Convert problematic values to string as fallback
                                    row_dict[col_name] = (
                                        str(value)
                                        if value is not None
                                        else None
                                    )
                        batch_data.append(row_dict)

                    # Insert batch into PostgreSQL temp table in smaller sub-batches
                    if batch_data:
                        # Use optimized sub-batch size from configuration
                        sub_batch_size = table_config.optimal_sub_batch_size

                        logger.info(
                            f"Using optimized sub-batch size of {sub_batch_size} for {len(columns)} columns"
                        )

                        with pg_engine.connect() as pg_conn:
                            # Build insert statement
                            temp_table_obj = Table(
                                temp_table,
                                MetaData(),
                                autoload_with=pg_engine,
                                schema=pg_config['schema'],
                            )

                            # Process in sub-batches with proper transaction handling
                            for i in range(0, len(batch_data), sub_batch_size):
                                sub_batch = batch_data[i : i + sub_batch_size]
                                if sub_batch:
                                    # Each sub-batch gets its own transaction
                                    trans = pg_conn.begin()
                                    try:
                                        pg_conn.execute(
                                            temp_table_obj.insert(), sub_batch
                                        )
                                        trans.commit()

                                        # Update progress tracking
                                        table_config.last_successful_offset = (
                                            total_processed + len(sub_batch)
                                        )
                                        table_config.temp_table_exists = True

                                    except Exception as e:
                                        trans.rollback()  # Rollback the failed transaction
                                        logger.warning(
                                            f"Bulk insert failed for sub-batch {i//sub_batch_size + 1}, trying row-by-row: {str(e)[:200]}"
                                        )

                                        # Try inserting one by one with individual transactions
                                        for row_idx, row in enumerate(
                                            sub_batch
                                        ):
                                            row_trans = pg_conn.begin()
                                            try:
                                                pg_conn.execute(
                                                    temp_table_obj.insert(),
                                                    [row],
                                                )
                                                row_trans.commit()
                                                # Update progress for each successful row
                                                table_config.last_successful_offset = (
                                                    total_processed
                                                    + i
                                                    + row_idx
                                                    + 1
                                                )
                                                table_config.temp_table_exists = (
                                                    True
                                                )
                                            except Exception as row_error:
                                                row_trans.rollback()
                                                logger.error(
                                                    f"Failed to insert row {i + row_idx} (IdOrder: {row.get('IdOrder', 'unknown')}): {str(row_error)[:200]}"
                                                )
                                                # Continue with next row instead of failing entirely
                                                continue

                                        # Periodically save progress
                        save_interval = (
                            migration_config.get(
                                'progress_save_interval', 10000
                            )
                            if 'migration_config' in locals()
                            else 10000
                        )
                        if total_processed % save_interval == 0:
                            table_config.processed_rows = total_processed
                            batch_manager.save_config()

                total_processed += len(rows)
                offset += chunk_size

                # Progress logging
                current_time = time.time()
                progress_pct = (
                    (total_processed / total_rows * 100)
                    if total_rows > 0
                    else 100
                )

                if (
                    current_time - last_log_time > 30
                    or progress_pct >= last_progress_pct + 10
                    or total_processed >= total_rows
                ):
                    elapsed = current_time - start_time
                    rows_per_sec = (
                        total_processed / elapsed if elapsed > 0 else 0
                    )
                    eta_seconds = (
                        (total_rows - total_processed) / rows_per_sec
                        if rows_per_sec > 0 and total_processed < total_rows
                        else 0
                    )

                    if total_processed >= total_rows:
                        logger.progress(
                            f"→ Completed: {total_processed:,}/{total_rows:,} rows - {rows_per_sec:.0f} rows/sec - Total time: {elapsed:.1f}s"
                        )
                    else:
                        logger.progress(
                            f"→ Progress: {progress_pct:.1f}% ({total_processed:,}/{total_rows:,}) - {rows_per_sec:.0f} rows/sec - ETA: {eta_seconds:.0f}s"
                        )

                    last_log_time = current_time
                    last_progress_pct = progress_pct

        finally:
            pass  # No sessions to close

        # Atomic operation: Replace final table with temp table
        logger.progress(
            f"Performing atomic table replacement for {table_name}..."
        )

        with pg_engine.begin() as trans:
            # Drop existing table if it exists
            trans.execute(
                text(
                    f'DROP TABLE IF EXISTS {pg_config["schema"]}."{table_name}"'
                )
            )
            # Rename temp table to final name
            trans.execute(
                text(
                    f'ALTER TABLE {pg_config["schema"]}."{temp_table}" RENAME TO "{table_name}"'
                )
            )

            # Clear temp table tracking since it's now the final table
            table_config.temp_table_exists = False
            table_config.temp_table_name = ""

        elapsed = time.time() - start_time
        success_rate = 1.0  # Assume success if we got here

        # Update batch configuration based on results
        batch_manager.update_config(
            table_name=table_name,
            success_rate=success_rate,
            processing_time=elapsed,
            total_rows=total_rows,
            processed_rows=total_processed,
            notes=f"Successful migration with {len(geo_columns)} geo columns",
        )

        logger.success(
            f"Migrated {table_name}: {total_processed:,} rows -> PostgreSQL ({elapsed:.1f}s)"
        )
        return True, table_name

    except Exception as e:
        elapsed = time.time() - start_time
        logger.failure(
            f"Failed to migrate {table_name} after {elapsed:.1f}s: {str(e)}"
        )

        # Update batch configuration to indicate failure
        try:
            if 'table_config' in locals():
                # Calculate partial success rate based on how far we got
                partial_success = (
                    (table_config.last_successful_offset / total_rows)
                    if 'total_rows' in locals() and total_rows > 0
                    else 0.0
                )
                batch_manager.update_config(
                    table_name=table_name,
                    success_rate=partial_success,
                    processing_time=elapsed,
                    total_rows=total_rows if 'total_rows' in locals() else 0,
                    processed_rows=table_config.last_successful_offset,
                    notes=f"Failed at {partial_success:.1%}: {str(e)[:100]}",
                )
                logger.info(
                    f"Saved partial progress: {table_config.last_successful_offset:,} rows successfully processed"
                )
        except:
            pass  # Don't let config update errors mask the original error

        # Decide whether to preserve temp table for resume
        preserve_for_resume = False
        threshold = (
            migration_config.get('preserve_temp_table_threshold', 1000)
            if 'migration_config' in locals()
            else 1000
        )
        if (
            'table_config' in locals()
            and table_config.last_successful_offset > threshold
        ):
            # Preserve temp table if we made significant progress
            preserve_for_resume = True
            table_config.temp_table_exists = True
            logger.info(
                f"Preserving temp table {temp_table} with {table_config.last_successful_offset:,} rows for resume"
            )

        # Cleanup temp table on error (unless preserving for resume)
        if temp_table:
            cleanup_temp_table(
                pg_config, temp_table, preserve_partial=preserve_for_resume
            )
            if not preserve_for_resume:
                if 'table_config' in locals():
                    table_config.temp_table_exists = False
                    table_config.temp_table_name = ""

        return False, table_name


def cleanup_temp_table(pg_config, temp_table, preserve_partial=False):
    """
    Clean up temporary table on error.

    If preserve_partial=True, keeps the temp table for resume functionality.
    """
    if preserve_partial:
        logger.info(
            f"Preserving temporary table {temp_table} for potential resume"
        )
        return

    try:
        conn_str = get_postgres_connection_string(pg_config)
        pg_engine = create_engine(conn_str)

        with pg_engine.begin() as trans:
            trans.execute(
                text(
                    f'DROP TABLE IF EXISTS {pg_config["schema"]}."{temp_table}"'
                )
            )
        logger.progress(f"Cleaned up temporary table: {temp_table}")
    except Exception as cleanup_error:
        logger.warning(
            f"Failed to cleanup temp table {temp_table}: {str(cleanup_error)}"
        )


def cleanup_all_temp_tables(pg_config):
    """
    Clean up all existing temporary tables before migration starts.

    This prevents bloated temp files when the script is interrupted.
    """
    try:
        conn_str = get_postgres_connection_string(pg_config)
        pg_engine = create_engine(conn_str)

        with pg_engine.begin() as trans:
            # Find all tables with _temp_ in their name
            result = trans.execute(
                text(
                    f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{pg_config["schema"]}' 
                AND table_name LIKE '%_temp_%'
                """
                )
            )
            temp_tables = [row[0] for row in result]

            if temp_tables:
                logger.progress(
                    f"Found {len(temp_tables)} temporary tables to clean up"
                )
                for i, temp_table in enumerate(temp_tables, 1):
                    logger.progress(
                        f"→ [{i}/{len(temp_tables)}] Dropping: {temp_table}"
                    )
                    trans.execute(
                        text(
                            f'DROP TABLE IF EXISTS {pg_config["schema"]}."{temp_table}"'
                        )
                    )
                logger.success(
                    f"Successfully cleaned up {len(temp_tables)} temporary tables"
                )
            else:
                logger.progress("No temporary tables found to clean up")

    except Exception as cleanup_error:
        logger.warning(f"Failed to cleanup temp tables: {str(cleanup_error)}")


def main():
    """
    Main migration function.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='SQL Server to PostgreSQL Migration Tool'
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=None,
        help=f'Number of concurrent threads for table processing (default: from config)',
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.yml',
        help='Path to YAML configuration file (default: config.yml)',
    )
    parser.add_argument(
        '--skip-log-tables',
        action='store_true',
        help='Skip tables containing "Log" in their name during migration',
    )
    args = parser.parse_args()

    logger.info("SQL Server to PostgreSQL Migration Tool")
    logger.info("=" * 50)

    # Load configuration from YAML
    yaml_config = load_yaml_config(args.config)
    config = get_sql_server_config(yaml_config)
    pg_config = get_postgres_config(yaml_config)
    migration_config = get_migration_config(yaml_config)

    # Initialize global batch manager
    global batch_manager
    batch_manager = BatchConfigManager()

    # Override threads from command line if provided
    max_threads = (
        args.threads
        if args.threads is not None
        else migration_config['max_threads']
    )

    logger.info(f"Server: {config['server']}:{config.get('port', 1433)}")
    logger.info(f"Database: {config['database']}")
    logger.info(f"Schema: {config['schema']}")

    logger.info(
        f"Target PostgreSQL: {pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
    )
    logger.info(f"Target Schema: {pg_config['schema']}")
    logger.info(
        f"Batch Config: chunk={migration_config['default_chunk_size']}, threads={max_threads}"
    )
    logger.info("")

    # Clean up any existing temporary tables from previous interrupted runs
    logger.info("Cleaning up any existing temporary tables...")
    cleanup_all_temp_tables(pg_config)
    logger.info("")

    try:
        # Connect to SQL Server
        logger.info("Connecting to SQL Server...")
        connection_string = get_connection_string(config)
        logger.info(f"Connection string: {connection_string}")
        logger.info("Available ODBC drivers:")
        for driver in pyodbc.drivers():
            logger.info(f"  - {driver}")

        conn = pyodbc.connect(connection_string, timeout=60)
        cursor = conn.cursor()
        logger.success("Connected successfully")

        # Create PostgreSQL schema
        if not create_postgres_schema(pg_config):
            logger.failure("Failed to create PostgreSQL schema. Exiting.")
            return

        # Get all tables in the schema
        logger.info(f"Fetching tables from schema '{config['schema']}'...")
        tables = get_schema_tables(cursor, config['schema'])

        if not tables:
            logger.warning(f"No tables found in schema '{config['schema']}'")
            return

        logger.info(f"Found {len(tables)} tables in SQL Server:")
        for table in tables:
            logger.info(f"  - {table}")
        logger.info("")

        # Filter out log tables if requested
        if args.skip_log_tables:
            original_count = len(tables)
            tables = [table for table in tables if 'Log' not in table]
            filtered_count = original_count - len(tables)
            if filtered_count > 0:
                logger.info(
                    f"Filtered out {filtered_count} log tables (containing 'Log')"
                )
                logger.info(f"Remaining {len(tables)} tables to process:")
                for table in tables:
                    logger.info(f"  - {table}")
                logger.info("")

        # Check existing tables and filter out those that already exist
        logger.info("=" * 50)
        logger.info("Checking existing tables in PostgreSQL...")

        # Get existing tables from PostgreSQL
        existing_pg_tables = get_existing_postgres_tables(pg_config)

        # Find missing tables (tables that need to be migrated)
        tables_to_migrate = [
            table for table in tables if table not in existing_pg_tables
        ]

        logger.info(
            f"Found {len(existing_pg_tables)} existing tables in PostgreSQL:"
        )
        for table in existing_pg_tables:
            logger.success(f"{table}")

        if tables_to_migrate:
            logger.info(f"\nFound {len(tables_to_migrate)} tables to migrate:")
            for table in tables_to_migrate:
                logger.info(f"  - {table}")
        else:
            logger.success(
                "All tables already exist in PostgreSQL - no migration needed!"
            )
            return

        logger.info("=" * 50)
        logger.info("")

        # Export each table using thread pool
        successful_exports = 0
        failed_exports = 0

        logger.info(f"Using {max_threads} concurrent threads for processing...")
        logger.info("")

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            # Submit all table export tasks
            future_to_table = {}
            for table in tables_to_migrate:
                future = executor.submit(
                    migrate_table_sqlalchemy,
                    config,
                    config['schema'],
                    table,
                    pg_config,
                )
                future_to_table[future] = table

            # Process completed tasks as they finish
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    success, table_name = future.result()
                    if success:
                        successful_exports += 1
                    else:
                        failed_exports += 1
                except Exception as e:
                    logger.failure(f"Thread error for {table}: {str(e)}")
                    failed_exports += 1

        # Summary
        logger.info("")
        logger.info("Migration Complete!")
        logger.success(f"Successfully exported: {successful_exports} tables")
        if failed_exports > 0:
            logger.failure(f"Failed exports: {failed_exports} tables")

        logger.info(
            f"Data migrated to PostgreSQL: {pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
        )
        logger.info(f"Schema: {pg_config['schema']}")

        # Save batch configuration for future runs
        logger.info("")
        batch_manager.save_config()

    except pyodbc.Error as e:
        logger.failure(f"Database connection error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.failure(f"Unexpected error: {str(e)}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed.")

        # Always save config even if migration failed
        try:
            batch_manager.save_config()
        except:
            pass  # Don't fail on config save errors


if __name__ == "__main__":
    main()

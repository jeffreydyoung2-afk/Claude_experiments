#!/usr/bin/env python3
"""
CSV to SQL Server Upsert Script

Reads CSV files from a network share, upserts into MainTable,
and maintains a HistoryTable with daily snapshots.

Usage:
    # Daily run (process latest unprocessed file)
    python csv_sql_upsert.py --server SQLSERVER --database MyDB --path "\\\\server\\share"

    # Full rebuild from specific date
    python csv_sql_upsert.py --server SQLSERVER --database MyDB --path "\\\\server\\share" \\
        --rebuild --start-date 01012026
"""

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyodbc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_sql_upsert.log')
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION - UPDATE THESE VALUES TO MATCH YOUR CSV SCHEMA
# =============================================================================

# Column definitions: column_name -> SQL Server type
# The first column should be your unique key
# NOTE: Do NOT include audit columns here - they are managed automatically
COLUMNS: Dict[str, str] = {
    'uniqueID': 'INT',
    'name': 'VARCHAR(255)',
    'value': 'DECIMAL(18,2)',
    'status': 'VARCHAR(50)',
}

# The unique key column for upsert operations
UNIQUE_KEY = 'uniqueID'

# Audit columns - automatically managed by the script
# These are added to both MainTable and HistoryTable
AUDIT_COLUMNS: Dict[str, str] = {
    'created_dt': 'DATETIME',
    'created_by': 'VARCHAR(100)',
    'modified_dt': 'DATETIME',
    'modified_by': 'VARCHAR(100)',
}

# System user for audit trail
SYSTEM_USER = 'CSV_UPSERT_SCRIPT'

# File pattern prefix
FILE_PREFIX = 'PartnerFile_'

# Date format in filename (MMDDYYYY)
DATE_FORMAT = '%m%d%Y'


@dataclass
class Config:
    """Configuration for the upsert script."""
    server: str
    database: str
    file_path: str
    schema: str = 'dbo'
    main_table: str = 'MainTable'
    history_table: str = 'HistoryTable'
    metadata_table: str = 'ProcessingMetadata'
    batch_size: int = 10000
    rebuild: bool = False
    start_date: Optional[str] = None
    retry_attempts: int = 2
    columns: Dict[str, str] = field(default_factory=lambda: COLUMNS.copy())
    audit_columns: Dict[str, str] = field(default_factory=lambda: AUDIT_COLUMNS.copy())
    unique_key: str = UNIQUE_KEY
    system_user: str = SYSTEM_USER

    @property
    def main_table_fq(self) -> str:
        """Fully qualified main table name."""
        return f"[{self.schema}].[{self.main_table}]"

    @property
    def history_table_fq(self) -> str:
        """Fully qualified history table name."""
        return f"[{self.schema}].[{self.history_table}]"

    @property
    def metadata_table_fq(self) -> str:
        """Fully qualified metadata table name."""
        return f"[{self.schema}].[{self.metadata_table}]"


def get_connection(config: Config) -> pyodbc.Connection:
    """Create a database connection using Windows Authentication."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={config.server};"
        f"DATABASE={config.database};"
        f"Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str, autocommit=False)
        logger.info(f"Connected to {config.server}/{config.database}")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def parse_date_from_filename(filename: str) -> Optional[datetime]:
    """Extract date from filename like PartnerFile_01132026.csv."""
    pattern = rf'{FILE_PREFIX}(\d{{8}})\.csv$'
    match = re.search(pattern, filename, re.IGNORECASE)
    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, DATE_FORMAT)
        except ValueError:
            logger.warning(f"Invalid date format in filename: {filename}")
    return None


def discover_files(config: Config, start_date: Optional[datetime] = None) -> List[Tuple[Path, datetime]]:
    """
    Discover CSV files matching the pattern and sort by date.

    Returns list of (file_path, date) tuples sorted by date ascending.
    """
    file_path = Path(config.file_path)
    if not file_path.exists():
        logger.error(f"Path does not exist: {file_path}")
        return []

    files_with_dates = []
    pattern = f"{FILE_PREFIX}*.csv"

    for csv_file in file_path.glob(pattern):
        file_date = parse_date_from_filename(csv_file.name)
        if file_date:
            if start_date is None or file_date >= start_date:
                files_with_dates.append((csv_file, file_date))
        else:
            logger.warning(f"Skipping file with unparseable date: {csv_file.name}")

    # Sort by date ascending
    files_with_dates.sort(key=lambda x: x[1])

    logger.info(f"Discovered {len(files_with_dates)} files to process")
    return files_with_dates


def get_last_processed_date(conn: pyodbc.Connection, config: Config) -> Optional[datetime]:
    """Get the last processed file date from metadata table."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT TOP 1 last_processed_date
            FROM {config.metadata_table_fq}
            ORDER BY last_processed_date DESC
        """)
        row = cursor.fetchone()
        if row:
            return row[0] if isinstance(row[0], datetime) else datetime.strptime(str(row[0]), '%Y-%m-%d')
        return None
    except pyodbc.Error:
        # Table might not exist yet
        return None


def update_last_processed_date(conn: pyodbc.Connection, config: Config, date: datetime):
    """Update the last processed date in metadata table."""
    cursor = conn.cursor()
    cursor.execute(f"""
        MERGE INTO {config.metadata_table_fq} AS target
        USING (SELECT 1 AS id) AS source
        ON 1=1
        WHEN MATCHED THEN UPDATE SET last_processed_date = ?
        WHEN NOT MATCHED THEN INSERT (last_processed_date) VALUES (?);
    """, date, date)
    conn.commit()


def ensure_tables_exist(conn: pyodbc.Connection, config: Config):
    """Create tables if they don't exist."""
    cursor = conn.cursor()

    # Build column definitions for data columns
    data_col_defs = ', '.join([f"[{col}] {dtype}" for col, dtype in config.columns.items()])

    # Build column definitions for audit columns
    audit_col_defs = ', '.join([f"[{col}] {dtype}" for col, dtype in config.audit_columns.items()])

    # Combine data and audit columns for MainTable
    main_col_defs = f"{data_col_defs}, {audit_col_defs}"
    main_col_defs_with_pk = main_col_defs.replace(
        f"[{config.unique_key}] {config.columns[config.unique_key]}",
        f"[{config.unique_key}] {config.columns[config.unique_key]} PRIMARY KEY"
    )

    # Create MainTable with audit columns
    cursor.execute(f"""
        IF OBJECT_ID('{config.schema}.{config.main_table}', 'U') IS NULL
        CREATE TABLE {config.main_table_fq} ({main_col_defs_with_pk})
    """)

    # Create HistoryTable with snapshot_date and audit columns
    history_col_defs = f"[snapshot_date] DATE NOT NULL, {data_col_defs}, {audit_col_defs}"
    cursor.execute(f"""
        IF OBJECT_ID('{config.schema}.{config.history_table}', 'U') IS NULL
        CREATE TABLE {config.history_table_fq} ({history_col_defs})
    """)

    # Create index on history table for efficient queries
    cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{config.schema}_{config.history_table}_snapshot_date')
        CREATE INDEX IX_{config.schema}_{config.history_table}_snapshot_date ON {config.history_table_fq} (snapshot_date)
    """)

    # Create metadata table
    cursor.execute(f"""
        IF OBJECT_ID('{config.schema}.{config.metadata_table}', 'U') IS NULL
        CREATE TABLE {config.metadata_table_fq} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            last_processed_date DATE NOT NULL
        )
    """)

    conn.commit()
    logger.info("Tables verified/created successfully")


def truncate_tables(conn: pyodbc.Connection, config: Config):
    """Truncate MainTable and HistoryTable for rebuild."""
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {config.main_table_fq}")
    cursor.execute(f"TRUNCATE TABLE {config.history_table_fq}")
    cursor.execute(f"TRUNCATE TABLE {config.metadata_table_fq}")
    conn.commit()
    logger.info("Tables truncated for rebuild")


def build_merge_statement(config: Config, num_rows: int) -> str:
    """Build a parameterized MERGE statement for batch upsert with audit columns."""
    # Data columns from CSV
    data_columns = list(config.columns.keys())
    data_col_list = ', '.join([f"[{c}]" for c in data_columns])

    # Build VALUES placeholders for the batch (only data columns from CSV)
    row_placeholder = '(' + ', '.join(['?' for _ in data_columns]) + ')'
    values_list = ', '.join([row_placeholder for _ in range(num_rows)])

    # Build UPDATE SET clause for data columns (exclude the unique key)
    update_data_cols = [c for c in data_columns if c != config.unique_key]
    update_data_set = ', '.join([f"target.[{c}] = source.[{c}]" for c in update_data_cols])

    # Add audit column updates for UPDATE (only modified_dt and modified_by)
    update_set = f"{update_data_set}, target.[modified_dt] = GETDATE(), target.[modified_by] = '{config.system_user}'"

    # All columns for INSERT (data + audit)
    all_insert_cols = data_columns + list(config.audit_columns.keys())
    insert_col_list = ', '.join([f"[{c}]" for c in all_insert_cols])

    # INSERT values: source data columns + audit defaults
    insert_values = ', '.join([f'source.[{c}]' for c in data_columns])
    insert_values += f", GETDATE(), '{config.system_user}', GETDATE(), '{config.system_user}'"

    merge_sql = f"""
        MERGE INTO {config.main_table_fq} AS target
        USING (
            SELECT * FROM (VALUES {values_list}) AS v ({data_col_list})
        ) AS source
        ON target.[{config.unique_key}] = source.[{config.unique_key}]
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_col_list}) VALUES ({insert_values});
    """
    return merge_sql


def process_batch(conn: pyodbc.Connection, config: Config, df: pd.DataFrame, attempt: int = 1):
    """Process a batch of rows using MERGE statement."""
    if df.empty:
        return

    columns = list(config.columns.keys())

    # Ensure DataFrame has all required columns
    for col in columns:
        if col not in df.columns:
            logger.warning(f"Column {col} not found in CSV, will use NULL")
            df[col] = None

    # Select only the columns we need in the right order
    df = df[columns]

    # Convert to list of tuples for pyodbc
    rows = df.values.tolist()
    params = [item for row in rows for item in row]  # Flatten

    try:
        cursor = conn.cursor()
        merge_sql = build_merge_statement(config, len(rows))
        cursor.execute(merge_sql, params)
        conn.commit()
        logger.debug(f"Processed batch of {len(rows)} rows")
    except pyodbc.Error as e:
        conn.rollback()
        if attempt < config.retry_attempts:
            logger.warning(f"Batch failed, retrying (attempt {attempt + 1}): {e}")
            process_batch(conn, config, df, attempt + 1)
        else:
            logger.error(f"Batch failed after {attempt} attempts: {e}")
            raise


def process_file(conn: pyodbc.Connection, config: Config, file_path: Path, file_date: datetime) -> bool:
    """
    Process a single CSV file with batched upserts.

    Returns True if successful, False otherwise.
    """
    logger.info(f"Processing file: {file_path.name} (date: {file_date.strftime('%Y-%m-%d')})")

    try:
        # Read CSV in chunks for memory efficiency
        chunks = pd.read_csv(
            file_path,
            chunksize=config.batch_size,
            dtype=str,  # Read all as strings, let SQL Server handle conversion
            na_values=['', 'NULL', 'null', 'NA', 'N/A'],
            keep_default_na=True
        )

        total_rows = 0
        for i, chunk in enumerate(chunks):
            # Replace NaN with None for SQL NULL
            chunk = chunk.where(pd.notnull(chunk), None)
            process_batch(conn, config, chunk)
            total_rows += len(chunk)
            if (i + 1) % 10 == 0:
                logger.info(f"  Processed {total_rows:,} rows so far...")

        logger.info(f"Completed processing {total_rows:,} rows from {file_path.name}")
        return True

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return False
    except pd.errors.EmptyDataError:
        logger.warning(f"Empty file: {file_path}")
        return False
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error for {file_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error processing {file_path}: {e}")
        return False


def snapshot_to_history(conn: pyodbc.Connection, config: Config, snapshot_date: datetime):
    """Copy current MainTable state to HistoryTable with snapshot date (including audit columns)."""
    cursor = conn.cursor()

    # Include both data columns and audit columns
    data_columns = list(config.columns.keys())
    audit_columns = list(config.audit_columns.keys())
    all_columns = data_columns + audit_columns
    col_list = ', '.join([f"[{c}]" for c in all_columns])

    cursor.execute(f"""
        INSERT INTO {config.history_table_fq} (snapshot_date, {col_list})
        SELECT ?, {col_list}
        FROM {config.main_table_fq}
    """, snapshot_date.date())

    row_count = cursor.rowcount
    conn.commit()
    logger.info(f"Snapshot saved to history: {row_count:,} rows for {snapshot_date.strftime('%Y-%m-%d')}")


def run_rebuild(config: Config):
    """Run full rebuild from start date."""
    if not config.start_date:
        logger.error("Start date required for rebuild mode")
        sys.exit(1)

    try:
        start_date = datetime.strptime(config.start_date, DATE_FORMAT)
    except ValueError:
        logger.error(f"Invalid start date format. Expected MMDDYYYY, got: {config.start_date}")
        sys.exit(1)

    logger.info(f"Starting REBUILD from {start_date.strftime('%Y-%m-%d')}")

    conn = get_connection(config)
    try:
        ensure_tables_exist(conn, config)
        truncate_tables(conn, config)

        files = discover_files(config, start_date)
        if not files:
            logger.warning("No files found to process")
            return

        processed_count = 0
        for file_path, file_date in files:
            if process_file(conn, config, file_path, file_date):
                snapshot_to_history(conn, config, file_date)
                update_last_processed_date(conn, config, file_date)
                processed_count += 1
            else:
                logger.warning(f"Skipping file due to errors: {file_path.name}")

        logger.info(f"Rebuild complete. Processed {processed_count}/{len(files)} files.")

    finally:
        conn.close()


def run_daily(config: Config):
    """Run daily incremental upsert."""
    logger.info("Starting DAILY incremental upsert")

    conn = get_connection(config)
    try:
        ensure_tables_exist(conn, config)

        last_processed = get_last_processed_date(conn, config)
        if last_processed:
            logger.info(f"Last processed date: {last_processed.strftime('%Y-%m-%d')}")
            start_date = last_processed + timedelta(days=1)
        else:
            logger.info("No previous processing found, processing all available files")
            start_date = None

        files = discover_files(config, start_date)
        if not files:
            logger.info("No new files to process")
            return

        # In daily mode, process files in order
        processed_count = 0
        for file_path, file_date in files:
            if process_file(conn, config, file_path, file_date):
                snapshot_to_history(conn, config, file_date)
                update_last_processed_date(conn, config, file_date)
                processed_count += 1
            else:
                logger.warning(f"Skipping file due to errors: {file_path.name}, continuing to next")

        logger.info(f"Daily run complete. Processed {processed_count}/{len(files)} new files.")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='CSV to SQL Server Upsert Script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Daily run (process latest unprocessed files)
  python csv_sql_upsert.py --server SQLSERVER --database MyDB --path "\\\\server\\share"

  # Full rebuild from specific date
  python csv_sql_upsert.py --server SQLSERVER --database MyDB --path "\\\\server\\share" \\
      --rebuild --start-date 01012026
        """
    )

    parser.add_argument('--server', required=True, help='SQL Server hostname')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--path', required=True, help='UNC path to CSV files')
    parser.add_argument('--schema', default='dbo', help='Database schema (default: dbo)')
    parser.add_argument('--main-table', default='MainTable', help='Main table name (default: MainTable)')
    parser.add_argument('--history-table', default='HistoryTable', help='History table name (default: HistoryTable)')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for processing (default: 10000)')
    parser.add_argument('--rebuild', action='store_true', help='Rebuild mode: truncate tables and process from start date')
    parser.add_argument('--start-date', help='Start date for rebuild (format: MMDDYYYY)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = Config(
        server=args.server,
        database=args.database,
        file_path=args.path,
        schema=args.schema,
        main_table=args.main_table,
        history_table=args.history_table,
        batch_size=args.batch_size,
        rebuild=args.rebuild,
        start_date=args.start_date
    )

    if config.rebuild:
        run_rebuild(config)
    else:
        run_daily(config)


if __name__ == '__main__':
    main()

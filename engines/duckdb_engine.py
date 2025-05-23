import logging
from typing import Optional

import duckdb
import os
import uuid
import pyarrow as pa

from engines.IEngine import IEngine, TABLE_NAMESPACE
from utils.sql_parser import extract_table_and_partitions

logger = logging.getLogger("__name__")

class DuckDbEngine(IEngine):
    def __init__(self, config: dict):
        super().__init__(config)
        self.con = duckdb.connect()
        self.warehouse_path = config.get("warehouse")

    def initialize_engine(self) -> None:
        """Initialize DuckDB with Iceberg extension and settings."""
        self.con.execute("INSTALL iceberg;")
        self.con.execute("LOAD iceberg;")
        self.con.execute("SET unsafe_enable_version_guessing=true;")

    def sync_views(self) -> None:
        """Create views for all tables in the catalog."""
        table_identifiers = self.catalog.list_tables("default")
        table_names = [tbl[1] for tbl in table_identifiers]

        # Setup the iceberg extension once before reflecting views
        self.initialize_engine()
        for table in table_names:
            full_table_name = f"{TABLE_NAMESPACE}.{table}"
            table_path = f"{self.warehouse_path}/{TABLE_NAMESPACE}.db/{table}"
            self.con.execute(
                f"""
                        CREATE OR REPLACE VIEW {table} AS
                        SELECT * FROM iceberg_scan(
                            '{table_path}', 
                            version='?',
                            allow_moved_paths=true
                        )
                        """
            )
            logger.info(f"Created view: {table} for table: {full_table_name}")

    def query(self, sql_command: str) -> pa.Table:
        """Execute a SQL command and return results as PyArrow table."""
        self.sync_views()
        try:
            return self.con.execute(sql_command).fetch_arrow_table()
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            raise

    def count(self, table_name: str) -> pa.Table:
        """Get count of rows in a table."""
        self.sync_views()
        try:
            count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            return count
        except Exception as e:
            logger.error(f"Error counting rows in table {table_name}: {e}")
            raise

    def get_changes_since_snapshot(self, table_name: str, snapshot_id: int) -> Optional[pa.Table]:
        """Get changes since the specified snapshot ID."""
        full_table_name = f"{TABLE_NAMESPACE}.{table_name}"

        try:
            if not self.catalog.table_exists(full_table_name):
                return None

            iceberg_table = self.catalog.load_table(full_table_name)
            current_snapshot = iceberg_table.current_snapshot()

            if not current_snapshot or current_snapshot.snapshot_id == snapshot_id:
                # No changes since the specified snapshot
                return None

            # Get the changes between snapshots using DuckDB's iceberg_snapshots function
            self.sync_views()
            changes = self.con.execute(
                f"""
                       WITH snapshot_data AS (
                           SELECT * FROM iceberg_snapshots('{self.warehouse_path}/{TABLE_NAMESPACE}.db/{table_name}')
                       )
                       SELECT s.* 
                       FROM {table_name} s
                       JOIN snapshot_data sd ON sd.snapshot_id > {snapshot_id} AND sd.snapshot_id <= {current_snapshot.snapshot_id}
                   """
            ).fetch_arrow_table()

            return changes
        except Exception as e:
            logger.error(
                f"Error getting changes for {table_name} since snapshot {snapshot_id}: {e}"
            )
            return None
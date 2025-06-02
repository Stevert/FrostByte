import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Union

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    LongType,
    NestedField,
    StringType,
    TimestampType,
    BooleanType,
    DoubleType,
    FloatType,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("__name__")


class IEngine(ABC):
    def __init__(self, config: dict):
        self.catalog_params: dict = config.get("catalog", {})
        self.warehouse_path = self.catalog_params.get("warehouse", "./tmp/iceberg")
        self.catalog_name = self.catalog_params.get("catalog_name", "default")
        self.catalog = load_catalog(self.catalog_name, **self.catalog_params)
        self.database = self.catalog_params.get("database_name", "default")

        namespaces = [ns[0] for ns in self.catalog.list_namespaces()]

        if self.database not in namespaces:
            self.catalog.create_namespace(self.database)

        self.initialize_engine()
        self.sync_views()

    @abstractmethod
    def initialize_engine(self) -> None:
        pass

    @abstractmethod
    def sync_views(self) -> None:
        pass

    @abstractmethod
    def query(self, sql_command: str) -> pa.Table:
        pass

    @abstractmethod
    def count(self, table_name: str) -> pa.Table:
        pass

    @property
    def tables(self) -> List[str]:
        """Get list of all tables in the namespace."""
        return [
            tbl_id[1] for tbl_id in self.catalog.list_tables((self.database,))
        ]

    @abstractmethod
    def get_schema(self, query):
        pass

    @abstractmethod
    def query_and_write(self, sql: str, result_dir: str) -> List[str]:
        pass

    def _convert_pyarrow_to_iceberg_type(
            self, pa_type
    ) -> Union[LongType, StringType, TimestampType, BooleanType, DoubleType, FloatType]:
        """Convert PyArrow type to Iceberg type."""
        if pa.types.is_int64(pa_type):
            return LongType()
        elif pa.types.is_string(pa_type):
            return StringType()
        elif pa.types.is_timestamp(pa_type):
            return TimestampType()
        elif pa.types.is_boolean(pa_type):
            return BooleanType()
        elif pa.types.is_float64(pa_type):
            return DoubleType()
        elif pa.types.is_float32(pa_type):
            return FloatType()
        else:
            # Default fallback
            logger.warning(f"Unsupported type: {pa_type}, defaulting to StringType")
            return StringType()

    def get_table(self, table_name) -> Table:
        logger.info(self.catalog.list_tables(self.database))
        logger.info(table_name)
        return self.catalog.load_table(self.get_full_table_name(table_name))

    def create_table(self, table_name: str, data: pa.Table) -> bool:
        """Create a new Iceberg table from PyArrow table."""
        full_table_name = self.get_full_table_name(table_name)

        if self.catalog.table_exists(full_table_name):
            logger.info(f"Table {full_table_name} already exists")
            return True

        fields = []
        for i, field in enumerate(data.schema, 1):
            iceberg_type = self._convert_pyarrow_to_iceberg_type(field.type)
            fields.append(
                NestedField(
                    i,
                    field.name,
                    iceberg_type,
                    required=False,
                )
            )

        schema = Schema(*fields)
        iceberg_table = self.catalog.create_table(
            identifier=full_table_name,
            schema=schema,
        )

        iceberg_table.append(data)
        logger.info(f"Created table: {full_table_name}")
        return True

    def insert(self, table_name: str, data: pa.Table) -> bool:
        """Insert data into an existing Iceberg table."""
        full_table_name = self.get_full_table_name(table_name)

        try:
            iceberg_table = self.catalog.load_table(full_table_name)
            iceberg_table.refresh()

            with iceberg_table.transaction() as transaction:
                transaction.append(data)

            logger.info(f"Inserted data into {full_table_name}: {len(data)} rows")
            return True
        except Exception as e:
            logger.error(f"Error inserting data into {full_table_name}: {e}")
            raise

    def get_current_snapshot_id(self, table_name: str) -> Optional[int]:
        """Get the current snapshot ID for a table if it exists."""
        full_table_name = self.get_full_table_name(table_name)

        try:
            if self.catalog.table_exists(full_table_name):
                iceberg_table = self.catalog.load_table(full_table_name)
                current_snapshot = iceberg_table.current_snapshot()
                if current_snapshot:
                    return current_snapshot.snapshot_id
            return None
        except Exception as e:
            logger.error(f"Error getting snapshot ID for {table_name}: {e}")
            return None

    @abstractmethod
    def get_changes_since_snapshot(
            self, table_name: str, snapshot_id: int
    ) -> Optional[pa.Table]:
        """Get changes since the specified snapshot ID."""
        pass

    def get_full_table_name(self, table_name) -> str:
        if self.database not in table_name:
            return f"{self.database}.{table_name}"
        return table_name

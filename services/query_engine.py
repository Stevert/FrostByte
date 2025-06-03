import logging
from typing import List

import pyarrow as pa
from pyarrow.flight import FlightStreamChunk

from engines.IEngine import IEngine
from engines.duckdb_engine import DuckDbEngine

logger = logging.getLogger()


class QueryEngineManager:
    def __init__(self, config: dict):
        self.config = config
        self.engine: IEngine = self.get_engine(config.get("query_engine", "duckdb"))
        self.result_path = config.get("result_path", "./tmp/results")

    def execute(self, query) -> pa.Table:
        return self.engine.query(query)

    def execute_and_save(self, query) -> List[str]:
        return self.engine.query_and_write(query, self.result_path)

    def write(self, table_name: str, flight_chunk: List[FlightStreamChunk], partitions: List[str]):
        batches = [chunk.data for chunk in flight_chunk]
        write_data = pa.Table.from_batches(batches)
        if table_name not in self.engine.tables:
            logger.info(f"Creating table: {table_name}")
            self.engine.create_table(table_name, write_data, partitions)
        else:
            logger.info(f"Inserting into table: {table_name}")
            self.engine.insert(table_name, write_data)

    def get_schema(self, table):
        if f"{table}" not in self.engine.tables:
            raise ValueError(f"Table {table} not found in the catalog.")

        return self.engine.get_schema(table)

    def get_table(self, table):
        if table not in self.engine.tables:
            raise ValueError(f"Table {table} not found in the catalog.")

        return self.engine.get_table(table)

    def get_engine(self, engine_type):
        if engine_type == "duckdb":
            return DuckDbEngine(self.config)
        logger.error(f"Unsupported engine type: {engine_type}")

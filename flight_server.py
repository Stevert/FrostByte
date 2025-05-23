import pyarrow.flight as flight
import pyarrow as pa
import os

from query_engine import QueryEngineManager
from utils.config_loader import load_config


class IcebergFlightServer(flight.FlightServerBase):
    def __init__(self, host, port, query_engine_manager):
        super().__init__(f"grpc://{host}:{port}")
        self.query_engine_manager = query_engine_manager

    def do_get(self, context, ticket):
        query = ticket.ticket.decode("utf-8")
        result_paths = self.query_engine_manager.execute(query)
        # Load results from paths
        record_batches = [pa.ipc.RecordBatchFileReader(pa.memory_map(p, 'r')).read_all() for p in result_paths]
        table = pa.concat_tables(record_batches)
        return flight.RecordBatchStream(table)

if __name__ == "__main__":
    config = load_config("config.yaml")
    query_engine_mgr = QueryEngineManager(config)
    server = IcebergFlightServer("0.0.0.0", 8815, query_engine_mgr)
    server.serve()

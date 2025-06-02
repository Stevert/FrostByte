import logging

import pyarrow.flight as flight
from pyarrow import ipc, RecordBatchReader

from logger_conf import logging_setup
from services.query_engine import QueryEngineManager
from utils.config_loader import load_config
from utils.file_utils import FileBatchReader

FLIGHT_PORT = 8816


class IcebergFlightServer(flight.FlightServerBase):

    def __init__(self, host, port, query_engine_manager: QueryEngineManager):
        super().__init__(f"grpc://{host}:{port}")
        self.query_engine_manager: QueryEngineManager = query_engine_manager

    '''def do_get(self, context, ticket):
        query = ticket.ticket.decode("utf-8")
        table = self.query_engine_manager.execute(query)
        logger.info(table)
        # Load results from paths
        # record_batches = [pa.ipc.RecordBatchFileReader(pa.memory_map(p, 'r')).read_all() for p in result_paths]
        # table = pa.concat_tables(record_batches)
        # return flight.RecordBatchStream(table)
        return flight.RecordBatchStream(table)'''

    def do_get(self, context, ticket):
        query = ticket.ticket.decode("utf-8")
        result_path = self.query_engine_manager.execute_and_save(query)[0]

        # Stream data from the Arrow file to the client
        f = open(result_path, "rb")  # Keep file handle open during stream
        reader = ipc.RecordBatchFileReader(f)

        # Stream batches via generator, this is safe and efficient
        def batch_generator():
            try:
                for i in range(reader.num_record_batches):
                    yield reader.get_batch(i)
            finally:
                f.close()

        return flight.RecordBatchStream(RecordBatchReader.from_batches(reader.schema, batches=batch_generator()))

    def do_put(self, context, descriptor, reader, writer):
        table_name = descriptor.path[0].decode("utf-8")
        try:
            batches = list(reader)
            if not batches:
                return
            self.query_engine_manager.write(table_name, batches)
        except Exception as e:
            logger.error(f"Error in do_put for table {table_name}", e)
            raise flight.FlightInternalError(f"Internal error: {str(e)}")

    def get_flight_info(self, context, descriptor):
        table_name = descriptor.path[0].decode("utf-8")
        try:
            ticket = f"SELECT * FROM {table_name}".encode("utf-8") if len(descriptor.path) == 1 else descriptor.path[1]
            location = flight.Location.for_grpc_tcp("localhost", FLIGHT_PORT)
            endpoints = [flight.FlightEndpoint(ticket, [location])]

            schema = self.query_engine_manager.get_schema(table_name)
            # logger.info(plan)
            # table: Table = self.query_engine_manager.get_table(table_name)
            # logger.info(table)
            return flight.FlightInfo(schema, descriptor, endpoints)
        except Exception as e:
            logger.error(f"Error in get_flight_info for table {table_name}: {e}", e)
            raise flight.FlightInternalError(f"Internal error: {str(e)}")


if __name__ == "__main__":
    logging_setup.configure()
    logger = logging.getLogger()
    config = load_config("config.yml")

    query_engine_mgr = QueryEngineManager(config)
    server = IcebergFlightServer("0.0.0.0", FLIGHT_PORT, query_engine_mgr)
    logger.info(f"Flight server started at grpc://localhost: {FLIGHT_PORT}")
    server.serve()

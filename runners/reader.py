import logging
import time

import pyarrow.flight as flight

from flight_server import FLIGHT_PORT

logger = logging.getLogger()


def run_reader(table_name: str, port: int, interval: int = 1):
    """Run a client that reads data from the IceRunner server."""
    client = flight.connect(f"grpc://localhost:{port}")
    logger.info(
        f"Reader connected to grpc://localhost:{port}, reading from {table_name}"
    )

    sql = f"SELECT * FROM {table_name} where id in (1748879424,1748879414,1748879404,174887)"
    while True:
        try:
            flight_info = client.get_flight_info(
                flight.FlightDescriptor.for_path(table_name.encode(), sql.encode())
            )
            endpoint = flight_info.endpoints[0]
            reader = client.do_get(endpoint.ticket)
            table = reader.read_all()
            count = len(table)
            logger.info(f"Current count: {count}")
        except Exception as e:
            logger.error(f"Error reading data: {e}")

        time.sleep(interval)


if __name__ == "__main__":
    table_name = "sample_table"
    run_reader(table_name, FLIGHT_PORT)

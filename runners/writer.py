import logging
import random
import time

import pyarrow as pa
import pyarrow.flight as flight

logger = logging.getLogger()


def run_writer(table_name: str, port: int, data=None):
    """Run a client that writes data to the IceRunner server."""
    client = flight.connect(f"grpc://localhost:{port}")
    logger.info(f"Writer connected to grpc://localhost:{port}, writing to {table_name}")

    while True:
        try:
            data = pa.Table.from_pylist(
                [{"id": int(time.time()), "value": f"val-{random.randint(100, 999)}"}],
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("value", pa.string(), nullable=False),
                    ]
                ),
            )

            writer, _ = client.do_put(
                flight.FlightDescriptor.for_path(table_name.encode()),
                data.schema,
            )
            writer.write_table(data)
            writer.close()

            logger.info(f"Uploaded data: {data.to_pydict()}")
        except Exception as e:
            logger.error(f"Error writing data: {e}")

        time.sleep(10)

import logging
import os
import uuid

from pyarrow import Table, ipc

logger = logging.getLogger()


def generate_unique_filename(extension="arrow"):
    return f"{uuid.uuid4()}.{extension}"


def write_arrow_file(arrow_table: Table, result_path: str) -> str:
    os.makedirs(result_path, exist_ok=True)
    filename = f"{result_path}/{generate_unique_filename()}"

    with ipc.new_file(filename, arrow_table.schema) as writer:
        writer.write_table(arrow_table)

    logger.info(f"Query result saved to {filename}")
    return filename

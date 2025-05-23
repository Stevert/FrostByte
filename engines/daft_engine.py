import uuid
import os
import daft
from daft import DataType
from daft.expressions import col

#import ray
#ray.init(ignore_reinit_error=True)

#@ray.remote
def execute_daft_worker(query, storage_path):
    df = daft.sql(query)
    table = df.to_arrow()
    path = os.path.join(storage_path, f"result_{uuid.uuid4().hex}.arrow")
    table.write_ipc(path)
    return path

def execute_daft(query, storage_path):
    parts = [f"{query} WHERE part = '{p}'" for p in range(4)]
    #futures = [execute_daft_worker.remote(p, storage_path) for p in parts]
    #return ray.get(futures)
    return execute_daft_worker(query, storage_path)

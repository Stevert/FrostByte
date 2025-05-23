from engines.duckdb_engine import  execute_query
from engines.daft_engine import execute_daft
import yaml

from engines.spark_engine import execute_spark
from utils.config_loader import load_config

class QueryEngineManager:
    def __init__(self, config):
        self.config = config
        self.engine = config.get("query_engine", "duckdb")
        self.result_path = config.get("result_path", "/tmp/results")

    def execute(self, query):
        result_files = []
        if self.engine == "duckdb":
            result_files = execute_query(query, self.result_path)
        elif self.engine == "daft":
            result_files = execute_daft(query, self.result_path)
        elif self.engine == 'spark':
            result_files = execute_spark(query, self.result_path)
        print("Results:", result_files)
        return result_files

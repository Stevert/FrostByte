import pyiceberg.catalog
import pyiceberg.catalog.glue
import pyiceberg.catalog.hive
import pyiceberg.catalog.sqlite

def create_catalog(catalog_type, config):
    if catalog_type == 'glue':
        return pyiceberg.catalog.glue.GlueCatalog(name="glue", **config)
    elif catalog_type == 'hive':
        return pyiceberg.catalog.hive.HiveCatalog(name="hive", **config)
    elif catalog_type == 'sqlite':
        return pyiceberg.catalog.sqlite.SQLiteCatalog(name="sqlite", **config)
    elif catalog_type == 'local':
        return pyiceberg.catalog.Catalog(name="local", **config)
    else:
        raise ValueError(f"Unsupported catalog type: {catalog_type}")
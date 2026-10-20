"""This package implements the tentaclio databricks database client"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _version

from tentaclio import *  # noqa

from .clients.databricks_client import DatabricksClient

try:
    __version__ = _version("tentaclio-databricks")
except PackageNotFoundError:
    __version__ = "0.0.0"

# Add DB registry
DB_REGISTRY.register("databricks+thrift", DatabricksClient)

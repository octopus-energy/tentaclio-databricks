
# tentaclio-databricks

A package containing all the dependencies for the `databricks+thrift` tentaclio schema .

## Quick Start

This project comes with a `Makefile` which is ready to do basic common tasks

```
$ make help
install                       Initalise the virtual env installing deps
clean                         Remove all the unwanted clutter
lock                          Lock dependencies
update                        Update dependencies (whole tree)
sync                          Install dependencies as per the lock file
lint                          Lint files with flake and mypy
format                        Run black and isort
test                          Run unit tests
circleci                      Validate circleci configuration (needs circleci cli)
```

## Configuring access to Databricks

Your connection url should be in the following format:

```
databricks+thrift://<token>@<host>?HTTPPath=<http_path>
```

Example values:
- token: dapi1213456789abc
- host: myhost.databricks.com
- http_path: /sql/1.0/endpoints/123456789

## Query Comments

Queries can be annotated with comments for observability using the `query_annotations` parameter.

### Example: Basic usage
```python
import os
from tentaclio import URL
from tentaclio_databricks.clients.databricks_client import DatabricksClient

url = URL("databricks+thrift://token@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123")
client = DatabricksClient(
    url,
    query_annotations={
        "app_name": "JupyterHub",
        "user": os.environ.get("USER_NAME", "unknown"),
        "pipeline_id": "456"
    }
)
```

### Result
All queries executed by the client will have prepended comments:
```sql
/* app_name='JupyterHub', user='john', pipeline_id='456' */
SELECT * FROM table
```

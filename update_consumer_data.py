import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from google.api_core.exceptions import Forbidden, GoogleAPIError
from google.cloud import bigquery

import utils

_LOGGER = logging.getLogger(__name__)
BIGQUERY_TOKEN = "BIGQUERY_TOKEN"


def _update_consumer_data(path: Path, bigquery_credentials: Path | None) -> None:
    today = datetime.fromisocalendar(*datetime.now(timezone.utc).isocalendar())
    table_suffix = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    # table_suffix = "2022-08-19"
    folder = path / table_suffix[0:4] / table_suffix[5:7]
    folder.mkdir(parents=True, exist_ok=True)
    file = folder / f"{table_suffix[8:10]}.csv"
    if file.exists():
        return

    _LOGGER.info(f"bigquery: fetching downloads for {table_suffix}")
    filters = []
    for filter_ in json.loads(utils.ROOT_PATH.joinpath("filters.json").read_text()):
        name, python_version = filter_.split("-")
        major, minor = python_version.split(".")
        filter_condition = (
            f'  else if (fn.startsWith("{name}-")) '
            f"{{ major = {major}; minor = {minor}; }}"
        )
        filters.append(filter_condition)
        if len(filters) >= 500:
            break
    query = rf'''
CREATE TEMP FUNCTION check_min_python_version(filename STRING, python_version STRING)
RETURNS BOOL
LANGUAGE js
AS r"""
  var major = 2;
  var minor = 0;
  const fn = filename.toLowerCase();
  if (false) {{ }}
{"\n".join(filters)}

  if ((major == 2) && (minor == 0)) return true;
  const parts = /^(?<major>\d+)\.(?<minor>\d+).*/.exec(python_version);
  if (!parts) return true;
  python_major = parseInt(parts.groups["major"], 10);
  python_minor = parseInt(parts.groups["minor"], 10);
  return (python_major > major) ||
         ((python_major == major) && (python_minor >= minor));
""";

SELECT t0.cpu, t0.num_downloads, t0.python_version, t0.pip_version, t0.glibc_version
FROM (SELECT COUNT(*) AS num_downloads,
REGEXP_EXTRACT(details.python, r"^([^\.]+\.[^\.]+)") as python_version,
REGEXP_EXTRACT(details.installer.version, r"^([^\.]+\.[^\.]+)") AS pip_version,
REGEXP_EXTRACT(details.distro.libc.version, r"^([^\.]+\.[^\.]+)") AS glibc_version,
details.cpu FROM bigquery-public-data.pypi.file_downloads WHERE
timestamp BETWEEN TIMESTAMP("{table_suffix} 00:00:00 UTC") AND
TIMESTAMP("{table_suffix} 23:59:59.999999 UTC") AND
details.installer.name = "pip" AND details.system.name = "Linux" AND
details.distro.libc.lib = "glibc" AND
REGEXP_CONTAINS(file.filename, r"-manylinux([0-9a-zA-Z_]+)\.whl") AND
check_min_python_version(file.filename, details.python)
GROUP BY pip_version, python_version, glibc_version, details.cpu
ORDER BY num_downloads DESC) AS t0;
'''

    with TemporaryDirectory() as temp:
        if bigquery_credentials is None:
            bigquery_credentials = Path(temp) / "key.json"
            bigquery_credentials.write_text(os.environ[BIGQUERY_TOKEN])
        with open(bigquery_credentials) as f:
            try:
                project = json.load(f)["project_id"]
                invalid = False
            except ValueError:
                invalid = True
        if invalid:
            raise ValueError("BIGQUERY_TOKEN is invalid")
        client = bigquery.Client.from_service_account_json(
            bigquery_credentials, project=project
        )
    query_job = client.query(query)
    try:
        rows = query_job.result()
    except Forbidden as e:
        if hasattr(e, "errors") and len(e.errors) > 0 and "message" in e.errors[0]:
            _LOGGER.warning(f'bigquery: {e.errors[0]["message"]}')
        else:
            _LOGGER.warning(f"bigquery: {e}")
        return
    except GoogleAPIError as e:
        _LOGGER.warning(f"bigquery: {e}")
        return
    if query_job.cache_hit:
        _LOGGER.debug("bigquery: using cached results")
    _LOGGER.info(f"bigquery: {query_job.total_bytes_billed // 1000000000} GB billed")
    with file.open("w") as f:
        f.write(",".join([f.name for f in rows.schema]) + "\n")
        for row in rows:
            f.write(",".join([str(field) for field in row]) + "\n")


def update(path: Path, bigquery_credentials: Path | None) -> None:
    if bigquery_credentials or os.environ.get(BIGQUERY_TOKEN, "") != "":
        _update_consumer_data(path, bigquery_credentials)

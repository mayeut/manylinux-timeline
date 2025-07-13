import csv
import json
import logging
import lzma
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from google.api_core.exceptions import Forbidden, GoogleAPIError
from google.cloud import bigquery

_LOGGER = logging.getLogger(__name__)
BIGQUERY_TOKEN = "BIGQUERY_TOKEN"


def _update_consumer_data(
    packages: list[str], path: Path, bigquery_credentials: Path | None
) -> None:
    today = datetime.fromisocalendar(*datetime.now(timezone.utc).isocalendar())
    table_suffix = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    # table_suffix = "2025-07-08"
    folder = path / table_suffix[0:4] / table_suffix[5:7]
    folder.mkdir(parents=True, exist_ok=True)
    file = folder / f"{table_suffix[8:10]}.csv.xz"
    if file.exists():
        return

    _LOGGER.info(f"bigquery: fetching downloads for {table_suffix}")
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
    job_config = None  # bigquery.QueryJobConfig(dry_run=True)
    total_bytes_processed = 0
    total_bytes_billed = 0
    packages_step = 2000
    csv_headers = None
    csv_rows = []
    # we need to split packages otherwise the project clustering does not kick-in...
    for project_start in range(0, len(packages), packages_step):
        # let's assume filtering on Linux is not necessary  since we're filtering on glibc:
        #   details.system.name = "Linux" AND
        # filtering on filename is quite costly, let's filter on file type instead:
        #   REGEXP_CONTAINS(file.filename, r"-manylinux([0-9a-zA-Z_]+)\.whl")
        #   file.type = "bdist_wheel"
        query = rf"""
SELECT t0.cpu, t0.num_downloads, t0.python_version, t0.glibc_version, t0.project
FROM (SELECT COUNT(*) AS num_downloads,
REGEXP_EXTRACT(details.python, r"^([^\.]+\.[^\.]+)") as python_version,
REGEXP_EXTRACT(details.distro.libc.version, r"^([^\.]+\.[^\.]+)") AS glibc_version,
details.cpu, project FROM bigquery-public-data.pypi.file_downloads WHERE
timestamp BETWEEN TIMESTAMP("{table_suffix} 00:00:00 UTC") AND
TIMESTAMP("{table_suffix} 23:59:59.999999 UTC") AND
project IN {tuple(packages[project_start:project_start+packages_step])} AND
details.distro.libc.lib = "glibc" AND
file.type = "bdist_wheel"
GROUP BY python_version, glibc_version, details.cpu, project
ORDER BY num_downloads DESC) AS t0;
"""
        query_job = client.query(query, job_config)
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
            _LOGGER.info("bigquery: using cached results")
        total_bytes_processed += query_job.total_bytes_processed or 0
        total_bytes_billed += query_job.total_bytes_billed or 0
        if not query_job.dry_run:
            csv_headers = [f.name for f in rows.schema]
            for row in rows:
                csv_rows.append([field for field in row])
    _LOGGER.info(f"bigquery: {total_bytes_processed // 1000000000} GB estimated")
    _LOGGER.info(f"bigquery: {total_bytes_billed // 1000000000} GB billed")
    csv_rows.sort(key=lambda x: x[1], reverse=True)  # sort by download count
    if csv_headers:
        with lzma.open(file, "wt", preset=9) as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(csv_headers)
            writer.writerows(csv_rows)


def update(packages: list[str], path: Path, bigquery_credentials: Path | None) -> None:
    if bigquery_credentials or os.environ.get(BIGQUERY_TOKEN, "") != "":
        _update_consumer_data(packages, path, bigquery_credentials)

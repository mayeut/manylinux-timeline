import json
import logging
import os

from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import requests

from google.api_core.exceptions import Forbidden, GoogleAPIError
from google.cloud import bigquery
from packaging.utils import canonicalize_name


_LOGGER = logging.getLogger(__name__)
BIGQUERY_TOKEN = 'BIGQUERY_TOKEN'


class _Item:
    def __init__(self, value):
        self.value = value
        self.value_ = canonicalize_name(value)

    def __hash__(self):
        return self.value_.__hash__()

    def __eq__(self, other):
        return self.value_.__eq__(other.value_)


def _merge(source, new_packages, packages_set):
    _LOGGER.debug(f'{source}: merging {len(new_packages)} package names')
    # need to handle canonicalize_name
    dst = set([_Item(value) for value in packages_set])
    src = set([_Item(value) for value in new_packages])
    subset = src - dst
    packages_set |= set([item.value for item in subset])
    _LOGGER.debug(f'{source}: now using {len(packages_set)} package names')


def _update_bigquery(bigquery_credentials, packages_set):
    _LOGGER.info('bigquery: fetching packages')
    today = datetime.fromisocalendar(*datetime.now(timezone.utc).isocalendar())
    table_suffix = (today - timedelta(days=2)).strftime('%Y%m%d')
    query = (
        'SELECT file.project AS project FROM '
        f'the-psf.pypi.downloads{table_suffix} WHERE '
        'REGEXP_CONTAINS(file.filename, "-manylinux\\\\w+\\\\.whl$") '
        'GROUP BY project'
    )
    with TemporaryDirectory() as temp:
        if bigquery_credentials is None:
            bigquery_credentials = Path(temp) / 'key.json'
            bigquery_credentials.write_text(os.environ[BIGQUERY_TOKEN])
        with open(bigquery_credentials) as f:
            project = json.load(f)['project_id']
        client = bigquery.Client.from_service_account_json(bigquery_credentials,
                                                           project=project)
    query_job = client.query(query)
    try:
        rows = query_job.result()
    except Forbidden as e:
        if hasattr(e, 'errors') and len(e.errors) > 0 and \
                'message' in e.errors[0]:
            _LOGGER.warning(f'bigquery: {e.errors[0]["message"]}')
        else:
            _LOGGER.warning(f'bigquery: {e}')
        return
    except GoogleAPIError as e:
        _LOGGER.warning(f'bigquery: {e}')
        return
    if query_job.cache_hit:
        _LOGGER.debug('bigquery: using cached results')
    _merge('bigquery', set(row.project for row in rows), packages_set)


def _update_top_packages(packages_set):
    _LOGGER.info('top pypi: fetching packages')
    response = requests.get('https://hugovk.github.io/top-pypi-packages/'
                            'top-pypi-packages-30-days.min.json')
    response.raise_for_status()
    top_packages_data = response.json()
    top_packages = set(row['project'] for row in top_packages_data['rows'])
    _LOGGER.debug(f'top pypi: merging {len(top_packages)} package names')
    packages_set |= top_packages
    _LOGGER.debug(f'top pypi: now using {len(packages_set)} package names')


def update(packages, use_top_packages, bigquery_credentials):
    packages_set = set(packages)
    if use_top_packages:
        _update_top_packages(packages_set)
    if bigquery_credentials or BIGQUERY_TOKEN in os.environ:
        _update_bigquery(bigquery_credentials, packages_set)
    return list(sorted(packages_set))

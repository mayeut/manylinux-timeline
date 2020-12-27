import logging
import lzma
import json
import os
import re

from datetime import date, datetime, timedelta
from typing import NamedTuple, Optional

import requests
import utils

from packaging.version import InvalidVersion, Version


_LOGGER = logging.getLogger(__name__)

_WHEEL_INFO_RE = re.compile(
    r"""^(?P<namever>(?P<name>.+?)-(?P<ver>.+?))(?:-(?P<build>\d[^-]*))?
     -(?P<pyver>.+?)-(?P<abi>.+?)-(?P<plat>.+?)\.whl$""",
    re.VERBOSE)

_WheelMetadata = NamedTuple('WheelMetadata', [
    ('name', str),
    ('version', str),
    ('build_tag', Optional[str]),
    ('implementation', str),
    ('abi', str),
    ('platform', str)
])


def _filter_versions(package, info, start, end):
    candidate_versions = []
    for version in info['releases'].keys():
        try:
            version_pep = Version(version)
            candidate_versions.append((version, version_pep))
        except InvalidVersion as e:
            _LOGGER.warning(f'{e} for {package}')

    candidate_versions.sort(key=lambda x: x[1], reverse=True)
    filtered = []
    weeks = set()
    for version, _ in candidate_versions:
        upload_date = date.max
        for file in info['releases'][version]:
            upload_date = min(
                upload_date,
                datetime.fromisoformat(file['upload_time']).date()
            )
        if upload_date < start:
            break
        if upload_date >= end:
            continue
        # at most one version per week, the more recent
        key = utils.to_week_str(upload_date)
        if key not in weeks:
            weeks.add(key)
            filtered.append(version)
    return filtered


def _parse_version(files):
    upload_date = date.max
    pythons = set()
    manylinux = set()
    for file in files:
        upload_date = min(upload_date,
                          datetime.fromisoformat(file['upload_time']).date())
        filename = file['filename']
        if not filename.lower().endswith('.whl'):
            continue
        parsed_filename = _WHEEL_INFO_RE.match(filename)
        if parsed_filename is None:
            _LOGGER.warning(f'invalid wheel name "{filename}"')
            continue  # invalid name
        metadata = _WheelMetadata(*parsed_filename.groups()[1:])
        if 'manylinux' not in metadata.platform:
            continue
        for python in metadata.implementation.replace(',', '.').split('.'):
            try:
                int(python[2:])
            except ValueError:
                _LOGGER.warning(
                    f'ignoring python "{python}" for wheel "{filename}"')
                continue
            pythons.add(python)
            if metadata.abi == 'abi3':
                assert python.startswith('cp3')
                # Add abi3 to know that cp3? > {python} are supported
                pythons.add('ab3')
        manylinux.add(metadata.platform)
    python_list = list(pythons)
    python_list.sort(key=lambda x: (int(x[2:]), x[0:2]))
    python_str = ".".join(python_list).replace('ab3', 'abi3')
    manylinux_str = ".".join(sorted(manylinux)).replace('anylinux', 'l')
    return utils.to_week_str(upload_date), python_str, manylinux_str


def _package_update(package, start, end, to_remove):
    _LOGGER.info(f'retrieving "{package}" info')
    response = requests.get(f'https://pypi.org/pypi/{package}/json')
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            _LOGGER.warning(f'{package} is not available on PyPI anymore')
            to_remove.add(package)
        else:
            _LOGGER.error(f'error {e} when retrieving {package} info')
        return []
    info = response.json()
    versions = _filter_versions(package, info, start, end)
    _LOGGER.debug(f'using "{versions}" for "{package}"')
    rows = []
    for version in versions[::-1]:
        week, python, manylinux = _parse_version(info['releases'][version])
        if python == '' or manylinux == '':
            continue
        rows.append(utils.Row(week, package, version, python, manylinux))
    if len(versions) and not len(rows):
        # to_remove.add(package)
        _LOGGER.warning(f'"{package} has no manylinux wheel in "{versions}"')
    return rows


def _load_rows():
    rows = []
    start = date.max
    end = date.min
    if os.path.exists(utils.CACHE_NAME):
        with lzma.open(utils.CACHE_NAME, 'rt') as f:
            for line in f:
                row = utils.Row(*line.strip().split(','))
                upload_date = utils.from_week_str(row.week)
                start = min(start, upload_date)
                end = max(end, upload_date)
                rows.append(row)
    end += utils.WEEK_DELTA
    return rows, start, end


def _save_rows(rows):
    rows.sort(key=lambda x: (x.python, x.manylinux, x.package, x.week))
    # rows.sort(key=lambda x: (x[1], x[2]))
    with lzma.open(utils.CACHE_NAME, 'wt') as f:
        for row in rows:
            f.write(f'{",".join(row)}\n')


def update(start_asked, end, use_top_packages):
    _LOGGER.debug('loading package list')
    with open('./packages.json') as f:
        packages = json.load(f)
    _LOGGER.debug(f'loaded {len(packages)} package names')
    top_packages = []
    if use_top_packages:
        _LOGGER.info('fetching top pypi packages')
        response = requests.get('https://hugovk.github.io/top-pypi-packages/'
                                'top-pypi-packages-30-days.min.json')
        response.raise_for_status()
        top_packages_data = response.json()
        _LOGGER.debug(f'merging {len(top_packages_data["rows"])} package names')
        top_packages = list(row['project'] for row in top_packages_data['rows'])
        top_packages = list(set(top_packages) - set(packages))
        top_packages.sort()
        _LOGGER.debug(f'now using {len(top_packages)} top package names')

    rows, start_cache, end_cache = _load_rows()
    # _save_rows(rows)
    # exit(0)
    start = start_asked - utils.WINDOW_SIZE
    if len(rows) > 0:
        if start < start_cache < end_cache < end:
            raise NotImplementedError(
                f'{start} < {start_cache} < {end_cache} < {end}')
        if start < start_cache <= end:
            end = start_cache
        if start <= end_cache < end:
            start = end_cache
        if start_cache <= start < end <= end_cache:
            _LOGGER.info(f'cache is up to date between {start} & {end}')
            return

    _LOGGER.info(f'updating timeline between {start} & {end}')
    to_remove = set()
    for package in packages + top_packages:
        rows.extend(_package_update(package, start, end, to_remove))
    packages.extend(set(top_packages) & set(r.package for r in rows))
    _save_rows(rows)
    packages = list(set(packages) - to_remove)
    packages.sort()
    with open('./packages.json', 'w') as f:
        json.dump(packages, f, separators=(',', ':'))

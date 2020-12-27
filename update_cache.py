import logging
import lzma
import json
import re

from datetime import date, datetime
from typing import NamedTuple, Optional

import feedparser
import requests
import utils

from packaging.version import InvalidVersion, Version


_LOGGER = logging.getLogger(__name__)

_WHEEL_INFO_RE = re.compile(
    r"""^(?P<namever>(?P<name>.+?)-(?P<ver>.+?))(?:-(?P<build>\d[^-]*))?
     -(?P<pyver>.+?)-(?P<abi>.+?)-(?P<plat>.+?)\.whl$""",
    re.VERBOSE)

_WheelMetadata = NamedTuple('_WheelMetadata', [
    ('name', str),
    ('version', str),
    ('build_tag', Optional[str]),
    ('implementation', str),
    ('abi', str),
    ('platform', str)
])

RELEASE_FEED_PATH = utils.CACHE_PATH / 'release_feed.json'


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


def _get_release_feed(package, to_remove, release_feed):
    url = f'https://pypi.org/rss/project/{package}/releases.xml'
    etag = None
    if package in release_feed.keys():
        etag = release_feed[package]['etag']
    feed = feedparser.parse(url, etag=etag)
    if feed.status == 404:
        _LOGGER.warning(f'"{package}": not available on PyPI anymore')
        to_remove.add(package)
        return None
    elif feed.bozo:
        _LOGGER.error(f'"{package}": error when retrieving release feed')
        return None
    if etag is None or feed.etag != etag:
        _LOGGER.debug(f'"{package}": update release feed cache')
        release_feed[package] = {
            'etag': feed.etag,
            'published': [date(*entry['published_parsed'][:3]).isoformat()
                          for entry in feed['entries']]
        }
    else:
        _LOGGER.debug(f'"{package}": use release feed cache')
    return release_feed[package]


def _has_new_release(package, start, end, to_remove, release_feed):
    feed = _get_release_feed(package, to_remove, release_feed)
    if feed is None:
        return False
    if len(feed['published']) == 0:
        return False
    published = [date.fromisoformat(entry) for entry in feed['published']]
    published_min = min(published)
    published_max = max(published)
    if published_max < start:
        return False
    if published_min > start:
        _LOGGER.debug(f'"{package}": assuming new release')
        return True
    return any([start <= p < end for p in published])


def _package_update(package, start, end, to_remove, release_feed):
    _LOGGER.info(f'"{package}": begin update')
    if not _has_new_release(package, start, end, to_remove, release_feed):
        _LOGGER.info(f'"{package}": no new release')
        return []
    return []
    response = requests.get(f'https://pypi.org/pypi/{package}/json')
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            _LOGGER.warning(f'"{package}": not available on PyPI anymore')
            to_remove.add(package)
        else:
            _LOGGER.error(f'"{package}": error "{e}" when retrieving info')
        return []
    info = response.json()
    versions = _filter_versions(package, info, start, end)
    _LOGGER.debug(f'"{package}": using "{versions}"')
    rows = []
    for version in versions[::-1]:
        week, python, manylinux = _parse_version(info['releases'][version])
        if python == '' or manylinux == '':
            continue
        rows.append(utils.Row(week, package, version, python, manylinux))
    if len(versions) and not len(rows):
        # to_remove.add(package)
        _LOGGER.warning(f'"{package}": no manylinux wheel in "{versions}"')
    _LOGGER.debug(f'"{package}": end update')
    return rows


def _load_rows():
    rows = []
    start = date.max
    end = date.min
    if utils.ROWS_PATH.exists():
        with lzma.open(utils.ROWS_PATH, 'rt') as f:
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
    with lzma.open(utils.ROWS_PATH, 'wt') as f:
        for row in rows:
            f.write(f'{",".join(row)}\n')


def _load_release_feed():
    release_feed = {}
    if RELEASE_FEED_PATH.exists():
        with open(RELEASE_FEED_PATH) as f:
            release_feed = json.load(f)
    return release_feed


def _save_release_feed(release_feed):
    with open(RELEASE_FEED_PATH, 'w') as f:
        json.dump(release_feed, f)


def update(start_asked, end, use_top_packages):
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
            #return
    else:
        end_cache = start

    _LOGGER.info(f'updating timeline between {start} & {end}')

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

    release_feed = _load_release_feed()
    to_remove = set()
    for package in packages + top_packages:
        rows.extend(_package_update(package, start, end, to_remove, release_feed))
    _save_release_feed(release_feed)
    packages.extend(set(top_packages) & set(r.package for r in rows))
    exit(0)
    _save_rows(rows)
    packages = list(set(packages) - to_remove)
    packages.sort()
    with open('./packages.json', 'w') as f:
        json.dump(packages, f, separators=(',', ':'))

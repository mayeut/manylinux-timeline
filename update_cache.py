import logging
import json
import urllib.parse

from datetime import date, datetime
from pathlib import Path
from shutil import move

import requests
import utils


_LOGGER = logging.getLogger(__name__)


def _build_url(package):
    return f'https://pypi.org/pypi/{package}/json'


def _package_update(package):
    headers = {'User-Agent': utils.USER_AGENT}
    package_new_name = package
    cache_file = utils.get_release_cache_path(package)
    if cache_file.exists():
        with open(cache_file) as f:
            info = json.load(f)
            headers['If-None-Match'] = info['etag']
    response = requests.get(_build_url(package), headers=headers)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            _LOGGER.warning(f'"{package}": not available on PyPI anymore')
            return None
        else:
            _LOGGER.error(f'"{package}": error "{e}" when retrieving info')
            return package_new_name  # keep package anyway
    for response_prev in response.history[::-1]:
        if response_prev.status_code == 301:
            new_location = response_prev.headers['location']
            uri = urllib.parse.urlparse(new_location)
            package_new_name = Path(uri.path).parent.name
            if _build_url(package_new_name) != new_location:
                _LOGGER.warning(f'"{package}": unsupported relocation')
                package_new_name = package
            else:
                _LOGGER.info(f'"{package}": new name "{package_new_name}"')
            break

    if response.status_code == 304:
        if package_new_name != package:
            cache_file_new = utils.get_release_cache_path(package_new_name)
            move(cache_file, cache_file_new)
        return package_new_name
    elif package_new_name != package:
        cache_file = utils.get_release_cache_path(package_new_name)

    info = response.json()
    # add 'etag' and filter-out what we don't need
    info = {'etag': response.headers['etag'], 'releases': info['releases']}
    for release in list(info['releases']):
        new_files = []
        upload_date_min = date.max
        for file in info['releases'][release]:
            upload_date = datetime.fromisoformat(file['upload_time']).date()
            upload_date_min = min(upload_date_min, upload_date)
            filename = file['filename']
            if not filename.lower().endswith('.whl'):
                continue
            parsed_filename = utils.WHEEL_INFO_RE.match(filename)
            if parsed_filename is None:
                _LOGGER.warning(f'"{package}":invalid wheel name "{filename}"')
                continue  # invalid name
            metadata = utils.WheelMetadata(*parsed_filename.groups()[1:])
            if 'manylinux' not in metadata.platform:
                continue
            new_files.append({'filename': filename,
                              'upload_time': upload_date.isoformat()})

        if len(new_files) > 0:
            new_files.insert(0, {'filename': 'ut-1.zip',
                                 'upload_time': upload_date_min.isoformat()})
            info['releases'][release] = new_files
        else:
            info['releases'].pop(release)
    with open(cache_file, 'w') as f:
        json.dump(info, f)
    return package_new_name


def update(packages, use_top_packages):
    utils.RELEASE_INFO_PATH.mkdir(exist_ok=True)

    packages_set = set(packages)
    if use_top_packages:
        _LOGGER.info('fetching top pypi packages')
        response = requests.get('https://hugovk.github.io/top-pypi-packages/'
                                'top-pypi-packages-30-days.min.json')
        response.raise_for_status()
        top_packages_data = response.json()
        _LOGGER.debug(f'merging {len(top_packages_data["rows"])} package names')
        top_packages = set(row['project'] for row in top_packages_data['rows'])
        packages_set |= top_packages
        _LOGGER.debug(f'now using {len(packages_set)} package names')

    to_remove = set()
    to_add = set()
    for package in sorted(packages_set):
        _LOGGER.info(f'"{package}": begin update')
        new_package = _package_update(package)
        if new_package is None:
            to_remove.add(package)
        elif new_package != package:
            to_remove.add(package)
            to_add.add(new_package)
        _LOGGER.debug(f'"{package}": end update')

    return list(sorted((packages_set - to_remove) | to_add))

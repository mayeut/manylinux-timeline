import json
import logging
import urllib.parse
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from multiprocessing.pool import ThreadPool
from pathlib import Path
from shutil import move
from typing import Any

import requests

import utils

_LOGGER = logging.getLogger(__name__)


class Status(Enum):
    PROCESSED = 1
    REMOVED = 2
    MOVED = 3
    ERROR = 4


@dataclass
class PackageStatus:
    name: str
    status: Status


def _build_url(package: str) -> str:
    return f"https://pypi.org/pypi/{package}/json"


def _check_cache_valid(info: Any) -> bool:
    # requires_python added
    for release in info["releases"]:
        for file in info["releases"][release]:
            if "requires_python" not in file:
                return False
            break
        break

    return True


def _package_update(package: str, handle_moved: bool = False) -> PackageStatus:
    _LOGGER.info(f'"{package}": begin update')
    headers = {"User-Agent": utils.USER_AGENT}
    package_new_name = package
    cache_file = utils.get_release_cache_path(package)
    if cache_file.exists():
        with open(cache_file) as f:
            try:
                info = json.load(f)
                if _check_cache_valid(info):
                    headers["If-None-Match"] = info["etag"]
            except json.JSONDecodeError:
                _LOGGER.warning(f'"{package}": cache corrupted')
    response = requests.get(_build_url(package), headers=headers)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            _LOGGER.warning(f'"{package}": not available on PyPI anymore')
            return PackageStatus(package, Status.REMOVED)
        else:
            _LOGGER.error(f'"{package}": error "{e}" when retrieving info')
            return PackageStatus(package, Status.ERROR)
    for response_prev in response.history[::-1]:
        if response_prev.status_code == 301:
            if not handle_moved:
                return PackageStatus(package, Status.MOVED)
            new_location = response_prev.headers["location"]
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
        return PackageStatus(package_new_name, Status.PROCESSED)
    elif package_new_name != package:
        cache_file = utils.get_release_cache_path(package_new_name)

    info = response.json()
    # add 'etag' and filter-out what we don't need
    info = {"etag": response.headers["etag"], "releases": info["releases"]}
    for release in list(info["releases"]):
        new_files = []
        upload_date_min = date.max
        for file in info["releases"][release]:
            upload_date = datetime.fromisoformat(file["upload_time"]).date()
            upload_date_min = min(upload_date_min, upload_date)
            filename = file["filename"]
            if not filename.lower().endswith(".whl"):
                continue
            parsed_filename = utils.WHEEL_INFO_RE.match(filename)
            if parsed_filename is None:
                _LOGGER.warning(f'"{package}":invalid wheel name "{filename}"')
                continue  # invalid name
            metadata = utils.WheelMetadata(*parsed_filename.groups()[1:])
            if "manylinux" not in metadata.platform:
                continue
            requires_python = file["requires_python"]
            new_files.append(
                {
                    "filename": filename,
                    "upload_time": upload_date.isoformat(),
                    "requires_python": requires_python,
                }
            )

        if len(new_files) > 0:
            new_files.insert(
                0,
                {
                    "filename": "ut-1.zip",
                    "upload_time": upload_date_min.isoformat(),
                    "requires_python": None,
                },
            )
            info["releases"][release] = new_files
        else:
            info["releases"].pop(release)
    with open(cache_file, "w") as f:
        json.dump(info, f)
    return PackageStatus(package_new_name, Status.PROCESSED)


def update(packages: list[str]) -> list[str]:
    utils.RELEASE_INFO_PATH.mkdir(exist_ok=True)
    packages_set = set(packages)
    to_remove = set()
    to_add = set()
    to_reprocess = set()

    with ThreadPool(32) as pool:
        for package_status in pool.imap(_package_update, sorted(packages), chunksize=1):
            if package_status.status == Status.PROCESSED:
                pass
            elif package_status.status == Status.REMOVED:
                to_remove.add(package_status.name)
            else:
                assert package_status.status in {Status.MOVED, Status.ERROR}
                to_reprocess.add(package_status.name)

    for package in sorted(to_reprocess):
        package_status = _package_update(package, handle_moved=True)
        if package_status.status == Status.REMOVED:
            to_remove.add(package_status.name)
        elif package_status.name != package:
            to_remove.add(package)
            to_add.add(package_status.name)

    return list(sorted((packages_set - to_remove) | to_add))

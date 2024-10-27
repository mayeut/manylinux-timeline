import functools
import json
import logging
import urllib.parse
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Any

import requests
from packaging.utils import canonicalize_name

import utils

_LOGGER = logging.getLogger(__name__)


class Status(Enum):
    PROCESSED = 1
    REMOVED = 2
    MOVED = 3
    ERROR = 4


@dataclass(frozen=True)
class PackageStatus:
    name: str
    status: Status
    etag: str | None = None
    expect_cache: bool = False


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


def _package_update(
    etag_cache: dict[str, tuple[str, bool]], package: str, handle_moved: bool = False
) -> PackageStatus:
    _LOGGER.info(f'"{package}": begin update')
    headers = {"User-Agent": utils.USER_AGENT}
    package_new_name = package
    package_etag_cache = etag_cache.get(package, None)
    if package_etag_cache is not None:
        headers["If-None-Match"] = package_etag_cache[0]

    try:
        response = requests.get(_build_url(package), headers=headers)
    except requests.exceptions.RequestException as e:
        _LOGGER.error(f'"{package}": error "{e}" when retrieving info')
        return PackageStatus(package, Status.ERROR)

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
            package_new_name = canonicalize_name(Path(uri.path).parent.name)
            assert package_new_name != package
            _LOGGER.info(f'"{package}": new name "{package_new_name}"')
            break

    cache_file = utils.get_release_cache_path(package)
    if response.status_code == 304:
        assert package_etag_cache is not None
        if package_new_name != package or (
            package_etag_cache[1] and not cache_file.exists()
        ):
            return _package_update({}, package_new_name, handle_moved=handle_moved)
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
    if len(info["releases"]) > 0:
        with open(cache_file, "w") as f:
            json.dump(info, f)
    return PackageStatus(
        package_new_name, Status.PROCESSED, info["etag"], len(info["releases"]) > 0
    )


def update(packages: list[str], all_pypi_packages: bool) -> list[str]:
    utils.RELEASE_INFO_PATH.mkdir(exist_ok=True)
    etag_cache_path = utils.CACHE_PATH / "etag_cache.json"

    etag_cache: dict[str, tuple[str, bool]] = {}
    if etag_cache_path.exists():
        with etag_cache_path.open() as f:
            etag_cache = json.load(f)

    new_etag_cache = etag_cache.copy()

    _LOGGER.info("Getting list of all PyPI packages ... ")
    headers = {
        "User-Agent": utils.USER_AGENT,
        "Accept": "application/vnd.pypi.simple.v1+json",
    }
    response = requests.get("https://pypi.org/simple/", headers=headers)
    response.raise_for_status()
    data = response.json()["projects"]
    all_packages = [canonicalize_name(project["name"]) for project in data]
    _LOGGER.info(f"Found {len(all_packages)} packages")
    packages_set = set(all_packages)

    if not all_pypi_packages:
        # remove packages without manylinux wheels
        packages_set.difference_update(
            name for name in etag_cache if not etag_cache[name][1]
        )
    # always add known packages, they'll be updated / removed if need be
    packages_set.update(name for name in etag_cache if etag_cache[name][1])
    packages_set.update(canonicalize_name(package) for package in packages)

    _LOGGER.info(f"Updating cache for {len(packages_set)} packages")

    _package_update_imap = functools.partial(_package_update, etag_cache)

    to_remove = set()
    to_add = set()
    to_reprocess = set()

    with ThreadPool(32) as pool:
        count = 0
        for package_status in pool.imap(
            _package_update_imap, sorted(packages_set), chunksize=1
        ):
            if package_status.etag is not None:
                new_etag_cache[package_status.name] = (
                    package_status.etag,
                    package_status.expect_cache,
                )
            if package_status.status == Status.PROCESSED:
                pass
            elif package_status.status == Status.REMOVED:
                to_remove.add(package_status.name)
                new_etag_cache[package_status.name] = ("", False)
            else:
                assert package_status.status in {Status.MOVED, Status.ERROR}
                to_reprocess.add(package_status.name)
            count += 1
            if count & 511 == 0:
                with etag_cache_path.open("w") as f:
                    json.dump(new_etag_cache, f, sort_keys=True, indent=2)

    for package in sorted(to_reprocess):
        package_status = _package_update(new_etag_cache, package, handle_moved=True)
        if package_status.etag is not None:
            new_etag_cache[package_status.name] = (
                package_status.etag,
                package_status.expect_cache,
            )
        if package_status.status == Status.REMOVED:
            to_remove.add(package_status.name)
            new_etag_cache[package_status.name] = ("", False)
        elif package_status.name != package:
            to_remove.add(package)
            new_etag_cache[package] = ("", False)
            to_add.add(package_status.name)

    with etag_cache_path.open("w") as f:
        json.dump(new_etag_cache, f, sort_keys=True, indent=2)

    to_remove.update(name for name in new_etag_cache if not new_etag_cache[name][1])

    return list(sorted((packages_set - to_remove) | to_add))

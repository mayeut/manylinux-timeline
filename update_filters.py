import gzip
import json
import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import requests
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

import update_dataset
import utils

_LOGGER = logging.getLogger(__name__)


@contextmanager
def sqlite3_connect(path: Path):
    try:
        con = sqlite3.connect(path)
        yield con
    finally:
        con.close()


def _filter_versions(package: str, info: dict) -> str | None:
    candidate_versions = []
    for version in info["releases"].keys():
        try:
            version_pep = Version(version)
            if version_pep.is_prerelease:
                _LOGGER.debug(f'"{package}": ignore pre-release {version}')
                continue
            candidate_versions.append((version, version_pep))
        except InvalidVersion as e:
            _LOGGER.warning(f'"{package}": {e}')

    if not candidate_versions:
        return None

    candidate_versions.sort(key=lambda x: x[1], reverse=True)

    return candidate_versions[0][0]


def _get_filter(
    files: list[dict[str, str]], last_python_requires: str | None
) -> str | None:
    names: set[str] = set()
    requires_pythons: set[SpecifierSet] = set()
    for file in files:
        filename = file["filename"]
        if not filename.lower().endswith(".whl"):
            continue
        parsed_filename = utils.WHEEL_INFO_RE.match(filename)
        if parsed_filename is None:
            continue
        metadata = utils.WheelMetadata(*parsed_filename.groups()[1:])
        names.add(metadata.name.lower())
        if file["requires_python"]:
            fixup_requires_python = file["requires_python"]
            fixup_requires_python = fixup_requires_python.replace(".*", "")
            fixup_requires_python = fixup_requires_python.replace("*", "")
            fixup_requires_python = fixup_requires_python.replace('"', "")
            fixup_requires_python = fixup_requires_python.replace("0<", "0,<")
            fixup_requires_python = fixup_requires_python.replace("3<", "3,<")
            try:
                requires_python = SpecifierSet(fixup_requires_python)
                requires_pythons.add(requires_python)
            except InvalidSpecifier:
                specifier_set = file["requires_python"]
                _LOGGER.warning(
                    f'invalid requires_python "{specifier_set}" for wheel "{filename}"'
                )

    if not names:
        return None

    assert len(names) == 1
    name = names.pop()
    python = "2.0"

    def _get_min_python(spec_sets: set[SpecifierSet]):
        for minor in range(6, 8):
            if any(f"2.{minor}" in spec_set for spec_set in spec_sets):
                return f"2.{minor}"
        for minor in range(0, 99):
            if any(f"3.{minor}" in spec_set for spec_set in spec_sets):
                return f"3.{minor}"
        return python

    if requires_pythons:
        python = _get_min_python(requires_pythons)
    else:
        # reuse update_dataset parsing
        _, pythons_str, _ = update_dataset.parse_version(files)
        pythons = pythons_str.split(".")
        if pythons[0] == "abi3":
            del pythons[0]
        if pythons[0] == "py2":
            python = "2.0"
        elif pythons[0] == "py32":
            python = "3.0"
        else:
            python = f"{pythons[0][2]}.{pythons[0][3:]}"
        python = python

    if last_python_requires:
        last_set = SpecifierSet(last_python_requires)
        if python not in last_set:
            python = _get_min_python({last_set})

    result = f"{name}-{python}"
    overrides = {
        "cython-2.7": "cython-3.6",  # no wheels below 3.6
        "opencv_python-3.6": "opencv_python-3.7",  # no wheels below 3.7
        "visualdl-2.7": "visualdl-3.0",  # pure wheel, no requires_python
        "parallel_ssh-2.7": "parallel_ssh-3.6",  # pure wheel, no requires_python
        "python_snappy-3.6": "python_snappy-3.8",  # pure wheel, no requires_python
        "tslearn-3.6": "tslearn-3.8",  # pure wheel, no requires_python
        "tensorrt-3.6": "tensorrt-3.8", # meta-packages, no requires_python
        "cobra-2.7": "cobra-3.8",  # pure wheel, no requires_python
    }
    return overrides.get(result, result)


def update() -> None:
    pypi_data_version = "2024.10.08"
    pypi_data_cache = utils.CACHE_PATH / f"pypi-{pypi_data_version}.db"
    if not pypi_data_cache.exists():
        _LOGGER.info("pypi data: download")
        db_url = (
            "https://github.com/sethmlarson/pypi-data/releases/download/"
            f"{pypi_data_version}/pypi.db.gz"
        )
        response = requests.get(db_url)
        response.raise_for_status()
        _LOGGER.info("pypi data: decompressing")
        pypi_data_cache.write_bytes(gzip.decompress(response.content))
    response = requests.get(
        "https://hugovk.github.io/top-pypi-packages/top-pypi-packages-30-days.min.json"
    )
    response.raise_for_status()
    top_packages_data = response.json()
    rows = sorted(
        top_packages_data["rows"], key=lambda x: x["download_count"], reverse=True
    )
    top_packages = [row["project"] for row in rows]
    filters = []
    with sqlite3_connect(pypi_data_cache) as con:
        for package in top_packages:
            package_norm = canonicalize_name(package)
            cache_file = utils.get_release_cache_path(package_norm)
            if not cache_file.exists():
                continue
            with open(cache_file) as f:
                info = json.load(f)
            version = _filter_versions(package, info)
            if version is None:
                continue
            query = "SELECT requires_python FROM packages WHERE name = ?"
            cur = con.execute(query, (package,))
            res = cur.fetchone()
            cur.close()
            assert res is not None
            python_requires = res[0]

            filter_ = _get_filter(info["releases"][version], python_requires)
            if filter_ is None:
                continue
            filters.append(filter_)
    with open(utils.ROOT_PATH / "filters.json", "w") as f:
        json.dump(filters, f, indent=0)
        f.write("\n")

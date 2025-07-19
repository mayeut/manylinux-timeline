import json
import logging
import re
from datetime import date

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

import utils

_LOGGER = logging.getLogger(__name__)
_FREE_THREADED_ABI = re.compile(r"cp3(1[3-9]|[2-9][0-9])t")


def _filter_versions(package: str, info: dict) -> list[str]:
    candidate_versions = []
    for version in info["releases"].keys():
        try:
            version_pep = Version(version)
            candidate_versions.append((version, version_pep))
        except InvalidVersion as e:
            _LOGGER.warning(f'"{package}": {e}')

    candidate_versions.sort(key=lambda x: x[1], reverse=True)
    filtered = []
    upload_date_previous_date = date.max.isoformat()
    for version, _ in candidate_versions:
        upload_date = date.max.isoformat()
        for file in info["releases"][version]:
            upload_date = min(upload_date, file["upload_time"])
        # Keep at most one version per day and do not keep maintenance branch
        # i.e dates shall be in same order as versions
        if upload_date < upload_date_previous_date:
            upload_date_previous_date = upload_date
            filtered.append(version)
    return filtered


def parse_version(files: list[dict[str, str]]) -> tuple[date, str, str]:
    upload_date = date.max.isoformat()
    pythons = set()
    manylinux = set()
    for file in files:
        upload_date = min(upload_date, file["upload_time"])
        filename = file["filename"]
        if not filename.lower().endswith(".whl"):
            continue
        parsed_filename = utils.WHEEL_INFO_RE.match(filename)
        if parsed_filename is None:
            continue
        requires_python: SpecifierSet | None = None
        if file["requires_python"]:
            fixup_requires_python = file["requires_python"]
            if fixup_requires_python in {"==3.10.4", "==3.10.5"}:
                fixup_requires_python = "==3.10"
            fixup_requires_python = fixup_requires_python.replace(".*", "")
            fixup_requires_python = fixup_requires_python.replace("*", "")
            fixup_requires_python = fixup_requires_python.replace('"', "")
            fixup_requires_python = fixup_requires_python.replace("0<", "0,<")
            fixup_requires_python = fixup_requires_python.replace("3<", "3,<")
            try:
                requires_python = SpecifierSet(fixup_requires_python)
            except InvalidSpecifier:
                specifier_set = file["requires_python"]
                _LOGGER.warning(
                    f'invalid requires_python "{specifier_set}" for wheel "{filename}"'
                )
        metadata = utils.WheelMetadata(*parsed_filename.groups()[1:])
        for python in metadata.implementation.replace(",", ".").split("."):
            if python.startswith("graalpy"):
                python = f"gp{python[7:]}"
            elif python.startswith("pyston"):
                python = f"pt{python[6:]}"

            try:
                int(python[2:])
            except ValueError:
                if not filename.startswith(
                    (
                        "pyswEOS-0",
                        "coremltools-0",
                        "gm-3.0.11",
                        "colesbury_c_extension-0.0.1-nogil39",
                        "pyarmor.mini-1.",
                        "simplex_solver-3.0.18-37-",
                        "voxec-0.4.1-cp$mm-",
                    )
                ):
                    _LOGGER.warning(
                        f'ignoring python "{python}" for wheel "{filename}"'
                    )
                continue

            if python.startswith(("cp", "pp", "gp", "pt")) and len(python) < 4:
                # minor is missing
                if not filename.startswith(
                    (
                        "mxlite_sdk-0.2.3-",
                        "mesh_to_depth-0.1.1-",
                        "pyarmor.mini-1.",
                        "pyomexmeta-1.2.3-",
                        "pyswEOS-0",
                        "simplex_solver-3.0.",
                    )
                ):
                    _LOGGER.warning(
                        f'ignoring python "{python}" for wheel "{filename}"'
                    )
                continue

            if python == "py3":
                if metadata.abi != "none":
                    if not filename.startswith(
                        ("enzyme_jax-0.0.4-", "kring-0.0.1-", "pyffmpeg-2.2.")
                    ):
                        _LOGGER.warning(
                            f"unsupported abi {metadata.abi!r} for wheel {filename!r}"
                        )
                    continue
                if requires_python is None:
                    python = "py32"
                else:
                    for minor in range(2, 99):
                        if f"3.{minor}" in requires_python:
                            python = f"py3{minor}"
                            break
                if python == "py3":
                    specifier_set = file["requires_python"]
                    if not filename.startswith(("kaldi_active_grammar-0", "pyswEOS-")):
                        _LOGGER.warning(
                            f"unresolved requires_python {specifier_set!r} "
                            f"for wheel {filename!r}"
                        )
                    continue
            pythons.add(python)

            if metadata.abi == "abi3":
                if not python.startswith("cp3"):
                    if not filename.startswith(
                        ("enzyme_jax-0.0.4-", "kring-0.0.1-", "pyffmpeg-2.2.")
                    ):
                        _LOGGER.warning(
                            f'ignoring python "{python}-abi3" for wheel "{filename}"'
                        )
                    continue
                # Add abi3 to know that cp3? > {python} are supported
                pythons.add("ab3")
            elif _FREE_THREADED_ABI.match(metadata.abi) or python.startswith("py3"):
                pythons.add("ft3")

        manylinux.add(metadata.platform)

    python_list = list(pythons)
    python_list.sort(key=lambda x: (int(x[2:]), x[0:2]))
    python_str = ".".join(python_list)
    python_str = python_str.replace("ab3", "abi3")
    python_str = python_str.replace("ft3", "free-threaded")
    manylinux_str = ".".join(sorted(manylinux)).replace("anylinux", "l")
    return date.fromisoformat(upload_date), python_str, manylinux_str


def _package_update(package: str) -> list[utils.Row]:
    cache_file = utils.get_release_cache_path(package)
    if not cache_file.exists():
        return []
    with open(cache_file) as f:
        info = json.load(f)

    versions = _filter_versions(package, info)
    _LOGGER.debug(f'"{package}": using "{versions}"')
    rows = []
    for version in versions:
        week, python, manylinux = parse_version(info["releases"][version])
        if python == "" or manylinux == "":
            continue
        rows.append(utils.Row(week, package, version, python, manylinux))
    if len(versions) and not len(rows):
        _LOGGER.warning(f'"{package}": no manylinux wheel in "{versions}"')
    return rows


def update(packages: list[str]) -> tuple[list[str], list[utils.Row]]:
    rows = []
    for package in packages:
        _LOGGER.info(f'"{package}": begin dataset creation')
        rows.extend(_package_update(package))
        _LOGGER.debug(f'"{package}": end dataset creation')
    return list(sorted({r.package for r in rows})), rows

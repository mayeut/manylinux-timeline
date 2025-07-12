import dataclasses
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Union, cast

import pandas as pd
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

import update_dataset
import utils

_LOGGER = logging.getLogger(__name__)

PYTHON_EOL = {
    "3.6": pd.to_datetime("2021-12-23"),
    "3.7": pd.to_datetime("2023-06-27"),
    "3.8": pd.to_datetime("2024-10-07"),
    "3.9": pd.to_datetime("2025-10-05"),
    "3.10": pd.to_datetime("2026-10-04"),
    "3.11": pd.to_datetime("2027-10-24"),
    "3.12": pd.to_datetime("2028-10-04"),
    "3.13": pd.to_datetime("2029-10-01"),
    "3.14": pd.to_datetime("2030-10-01"),
}


def _get_major_minor(x):
    try:
        version = Version(x)
    except InvalidVersion:
        return "0.0"
    if version.major > 50:
        return "0.0"  # invalid version
    return f"{version.major}.{version.minor}"


def _load_df(
    wheel_support_map: dict[str, dict[str, date]], path: Path, date_: date
) -> pd.DataFrame | None:
    folder = path / date_.strftime("%Y") / date_.strftime("%m")
    file = folder / f"{date_.strftime('%d')}.csv"
    file_xz = file.with_suffix(".csv.xz")
    usecols = ["num_downloads", "python_version", "glibc_version"]
    if file_xz.exists():
        usecols.append("project")
        file = file_xz
    if not file.exists():
        return None
    df = pd.read_csv(
        file,
        converters={
            "python_version": lambda x: _get_major_minor(x),
            "glibc_version": lambda x: _get_major_minor(x),
            "project": lambda x: str(canonicalize_name(x)),
        },
        usecols=usecols,
    )
    df["day"] = pd.to_datetime(date_)
    # remove unneeded python version
    df.query("python_version in @PYTHON_EOL", inplace=True)
    # check if the package is supported or not for a given python version
    if "project" in usecols:

        def _get_supported_wheel(row, *, src_column: str) -> str:
            value = row[src_column]
            project = wheel_support_map.get(row["project"])
            if project is None:
                _LOGGER.warning(f"{project} not found in wheel_support_map")
                return value
            max_date = project[row["python_version"]]
            if date_ < max_date:
                return value
            if row["python_version"] == "3.12":
                return f"{value}-nsw"  # no supported wheel
            return f"{value}-nsw"  # no supported wheel

        df["python_version2"] = df.apply(
            _get_supported_wheel, axis=1, src_column="python_version"
        )
        df["glibc_version"] = df.apply(
            _get_supported_wheel, axis=1, src_column="glibc_version"
        )

        df = (
            df.drop(["project"], axis=1)
            .groupby(
                ["day", "python_version", "python_version2", "glibc_version"],
                as_index=False,
            )
            .aggregate("sum")
        )
    else:
        df["python_version2"] = df["python_version"]
    return df


@dataclasses.dataclass
class SupportDates:
    supported: date = date.min
    not_supported: list[date] = dataclasses.field(default_factory=list)


def _build_wheel_support_map(packages: list[str]) -> dict[str, dict[str, date]]:
    result: dict[str, dict[str, date]] = {}
    for package in packages:
        cache_file = utils.get_release_cache_path(package)
        if not cache_file.exists():
            result[package] = {version: date.max for version in PYTHON_EOL}
            continue
        info = json.loads(cache_file.read_text())
        package_result: dict[str, SupportDates] = {
            version: SupportDates() for version in PYTHON_EOL
        }
        only_prerelease = True
        for release in info["releases"]:
            try:
                version_pep = Version(release)
                if not version_pep.is_prerelease:
                    only_prerelease = False
                    break
            except InvalidVersion as e:
                _LOGGER.warning(f'"{package}": {e}')
        for release, release_files in info["releases"].items():
            try:
                version_pep = Version(release)
                if version_pep.is_prerelease and not only_prerelease:
                    _LOGGER.debug(f'"{package}": ignore pre-release {release}')
                    continue
            except InvalidVersion:
                pass
            # check if file could be installed
            upload_date, pythons_str, _ = update_dataset.parse_version(release_files)
            if not pythons_str:
                continue
            pythons = pythons_str.split(".")
            pythons = list(filter(lambda x: not x.startswith("py2"), pythons))
            if not pythons:
                continue
            if pythons[0] == "abi3":
                del pythons[0]  # remove the tag & replace with supported versions
                cp3_start = next(
                    python for python in pythons if python.startswith("cp3")
                )
                start_minor = int(cp3_start[3:])
                for key in PYTHON_EOL:
                    key_minor = int(key[2:])
                    if key_minor > start_minor:
                        pythons.append(f"cp3{key_minor}")
            if any(python.startswith("py3") for python in pythons):
                py3_start = next(
                    python for python in pythons if python.startswith("py3")
                )
                start_minor = int(py3_start[3:])
                for key in PYTHON_EOL:
                    key_minor = int(key[2:])
                    if key_minor > start_minor:
                        pythons.append(f"py3{key_minor}")
            supported_set = {f"{python[2]}.{python[3:]}" for python in pythons}
            for key in PYTHON_EOL:
                if key in supported_set:
                    package_result[key].supported = max(
                        package_result[key].supported, upload_date
                    )
                else:
                    package_result[key].not_supported.append(upload_date)
        if package == "soundfile":
            package = package
        result[package] = {}
        previous_date = date.min
        for key in PYTHON_EOL:
            if package_result[key].supported == date.min:
                if len(package_result[key].not_supported) == 0:
                    if package not in {
                        "carbonara-pyvex",
                        "libaio-bins",
                        "ms-ivy",
                        "nighres",
                        "oneqloud-polynomials",
                        "sciunit2",
                        "simuvex",
                        "tesseract-python",
                    }:
                        _LOGGER.warning(f"{package}: assume python {key} supported")
                    result[package][key] = date.max
                else:
                    result[package][key] = min(package_result[key].not_supported)
            else:
                package_result[key].not_supported.append(date.max)
                result[package][key] = min(
                    filter(
                        lambda x: x > package_result[key].supported,
                        package_result[key].not_supported,
                    )
                )
            result[package][key] = previous_date = max(
                previous_date, result[package][key]
            )

    return result


def update(packages: list[str], path: Path, start: date, end: date):
    date_ = start - utils.CONSUMER_WINDOW_SIZE
    dataframes = []
    wheel_support_map = _build_wheel_support_map(packages)
    while date_ < end:
        df = _load_df(wheel_support_map, path, date_)
        if df is not None:
            dataframes.append(df)
        date_ = date_ + timedelta(days=1)

    df = pd.concat(dataframes)
    df = df.groupby(
        ["day", "python_version", "python_version2", "glibc_version"], as_index=False
    ).aggregate("sum")

    # apply rolling window
    df = pd.pivot_table(
        df,
        index="day",
        columns=["python_version", "python_version2", "glibc_version"],
        values="num_downloads",
        fill_value=0,
        aggfunc="sum",
    )
    df = df.rolling(window=utils.CONSUMER_WINDOW_SIZE, min_periods=1).sum()
    df = (
        df.stack(list(range(df.columns.nlevels)), future_stack=True)
        .reset_index()
        .fillna(0.0)
    )
    df.rename(columns={0: "num_downloads"}, inplace=True)
    df = df[(df["num_downloads"] > 0) & (df["day"] >= pd.to_datetime(start))]
    df = df.groupby(
        ["day", "python_version", "python_version2", "glibc_version"], as_index=False
    ).aggregate("sum")

    # non EOL dataframe
    df_non_eol = df.copy()
    for k, v in PYTHON_EOL.items():
        mask = (df_non_eol["python_version"] == k) & (df_non_eol["day"] >= v)
        df_non_eol.loc[mask, "num_downloads"] = 0

    df.set_index("day", append=True, inplace=True)
    df = df.swaplevel()

    df_non_eol.set_index("day", append=True, inplace=True)
    df_non_eol = df_non_eol.swaplevel()

    # python version download stats
    df_python = (
        df[["python_version2", "num_downloads"]]
        .groupby(["day", "python_version2"])
        .aggregate("sum")
    )
    df_python_all = df_python.groupby(["day"]).aggregate("sum")
    df_python_stats = df_python / df_python_all

    df_python_non_eol = (
        df_non_eol[["python_version2", "num_downloads"]]
        .groupby(["day", "python_version2"])
        .aggregate("sum")
    )
    df_python_non_eol_all = df_python_non_eol.groupby(["day"]).aggregate("sum")
    df_python_non_eol_stats = df_python_non_eol / df_python_non_eol_all

    # glibc version download stats
    df_glibc = (
        df[["glibc_version", "num_downloads"]]
        .groupby(["day", "glibc_version"])
        .aggregate("sum")
    )
    df_glibc_all = df_glibc.groupby(["day"]).aggregate("sum")
    df_glibc_stats = df_glibc / df_glibc_all

    df_glibc_non_eol = (
        df_non_eol[["glibc_version", "num_downloads"]]
        .groupby(["day", "glibc_version"])
        .aggregate("sum")
    )
    df_glibc_non_eol_all = df_glibc_non_eol.groupby(["day"]).aggregate("sum")
    df_glibc_non_eol_stats = df_glibc_non_eol / df_glibc_non_eol_all

    out: dict[str, Any] = {
        "last_update": datetime.now(timezone.utc).strftime("%A, %d %B %Y, %H:%M:%S %Z"),
        "index": list(d.date().isoformat() for d in df_python_all.index),
    }

    # combine some versions to remove some of the less used ones
    # but still accounting for the smaller one
    glibc_versions = [
        tuple(f"2.{minor}" for minor in range(5, 17)),
        tuple(f"2.{minor}" for minor in range(17, 26)),
        ("2.26",),
        ("2.27",),
        ("2.28", "2.29", "2.30"),
        ("2.31", "2.32", "2.33"),
        ("2.34",),
        ("2.35",),
        ("2.36", "2.37", "2.38"),
        ("2.39", "2.40", "2.41"),
    ]
    glibc_versions = glibc_versions[::-1]
    glibc_version = dict[str, Union[list[str], list[float]]]()
    glibc_version["keys"] = list(
        f"{v[0]}{suffix}" for v in glibc_versions for suffix in ("", "-nsw")
    )
    glibc_version_non_eol = dict[str, Union[list[str], list[float]]]()
    glibc_version_non_eol["keys"] = glibc_version["keys"]
    for versions in glibc_versions:
        for suffix in ("", "-nsw"):
            stats = []
            stats_non_eol = []
            for day in out["index"]:
                value = 0.0
                value_non_eol = 0.0
                for version in versions:
                    version_suffix = f"{version}{suffix}"
                    try:
                        value += float(
                            df_glibc_stats.loc[
                                (pd.to_datetime(day), version_suffix), "num_downloads"
                            ]
                        )
                    except KeyError:
                        pass
                    try:
                        value_non_eol += float(
                            df_glibc_non_eol_stats.loc[
                                (pd.to_datetime(day), version_suffix), "num_downloads"
                            ]
                        )
                    except KeyError:
                        pass
                stats.append(float(f"{100.0 * value:.2f}"))
                stats_non_eol.append(float(f"{100.0 * value_non_eol:.2f}"))
            glibc_version[f"{versions[0]}{suffix}"] = stats
            glibc_version_non_eol[f"{versions[0]}{suffix}"] = stats_non_eol

    out["glibc_version"] = glibc_version
    out["glibc_version_non_eol"] = glibc_version_non_eol

    python_versions = [f"{v}{suffix}" for v in PYTHON_EOL for suffix in ("-nsw", "")]
    python_version = dict[str, list[str] | list[float]]()
    python_version_non_eol = dict[str, list[str] | list[float]]()
    glibc_readiness = dict[str, dict[str, list[str] | list[float]]]()
    python_version["keys"] = python_versions
    python_version_non_eol["keys"] = [
        version
        for version in python_versions
        if PYTHON_EOL[version.split("-")[0]] > pd.to_datetime(start)
    ]
    for version in python_versions:
        stats = []
        for day in out["index"]:
            try:
                value = float(
                    df_python_stats.loc[(pd.to_datetime(day), version), "num_downloads"]
                )
            except KeyError:
                value = 0.0
            stats.append(float(f"{100.0 * value:.1f}"))
        python_version[version] = stats
        if version in python_version_non_eol["keys"]:
            stats = []
            for day in out["index"]:
                try:
                    value = float(
                        df_python_non_eol_stats.loc[
                            (pd.to_datetime(day), version), "num_downloads"
                        ]
                    )
                except KeyError:
                    value = 0.0
                stats.append(float(f"{100.0 * value:.1f}"))
            python_version_non_eol[version] = stats

        if version.endswith("-nsw"):
            continue

        df_python_version = df[df["python_version"] == version]
        df_glibc = (
            df_python_version[["glibc_version", "num_downloads"]]
            .groupby(["day", "glibc_version"])
            .aggregate("sum")
        )
        df_glibc_all = df_glibc.groupby(["day"]).aggregate("sum")
        df_glibc_stats = df_glibc / df_glibc_all
        glibc_readiness_ver = dict[str, Union[list[str], list[float]]]()
        glibc_readiness_ver["keys"] = cast(list[str], list(glibc_version["keys"]))
        glibc_readiness[version] = glibc_readiness_ver
        for versions in glibc_versions:
            for suffix in ("", "-nsw"):
                stats = []
                for day in out["index"]:
                    value = 0.0
                    for glibc_version_ in versions:
                        version_suffix = f"{glibc_version_}{suffix}"
                        try:
                            value += float(
                                df_glibc_stats.loc[
                                    (pd.to_datetime(day), version_suffix),
                                    "num_downloads",
                                ]
                            )
                        except KeyError:
                            pass
                    stats.append(float(f"{100.0 * value:.2f}"))
                glibc_readiness_ver[f"{versions[0]}{suffix}"] = stats
    # remove all zeros "-nsw" entries
    for key in list(python_version["keys"]):
        assert isinstance(key, str)
        if not key.endswith("-nsw"):
            continue
        if all(value == 0.0 for value in python_version[key]):
            python_version["keys"].remove(key)  # type: ignore
            python_version.pop(key)
            glibc_readiness_ver = glibc_readiness[key.rstrip("-nsw")]
            for glibc_key in list(glibc_readiness_ver["keys"]):
                assert isinstance(glibc_key, str)
                if glibc_key.endswith("-nsw"):
                    glibc_readiness_ver["keys"].remove(glibc_key)  # type: ignore
                    glibc_readiness_ver.pop(glibc_key)

    for key in list(python_version_non_eol["keys"]):
        assert isinstance(key, str)
        if not key.endswith("-nsw"):
            continue
        if all(value == 0.0 for value in python_version_non_eol[key]):
            python_version_non_eol["keys"].remove(key)  # type: ignore
            python_version_non_eol.pop(key)

    out["python_version"] = python_version
    out["python_version_non_eol"] = python_version_non_eol
    out["glibc_readiness"] = glibc_readiness

    with utils.CONSUMER_DATA_PATH.open("w") as f:
        json.dump(out, f, separators=(",", ":"))

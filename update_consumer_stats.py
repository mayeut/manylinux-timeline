import dataclasses
import functools
import json
import logging
import multiprocessing
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, cast

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

import pandas as pd
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

import update_dataset
import utils

_LOGGER = logging.getLogger(__name__)

PYTHON_EOL: Final[dict[str, pd.Timestamp]] = {
    "3.7": pd.to_datetime("2023-06-27"),
    "3.8": pd.to_datetime("2024-10-07"),
    "3.9": pd.to_datetime("2025-10-31"),
    "3.10": pd.to_datetime("2026-10-31"),
    "3.11": pd.to_datetime("2027-10-31"),
    "3.12": pd.to_datetime("2028-10-31"),
    "3.13": pd.to_datetime("2029-10-31"),
    "3.14": pd.to_datetime("2030-10-31"),
    "3.15": pd.to_datetime("2031-10-31"),
}

# combine some glibc versions to remove some of the less used ones
# but still accounting for the smaller one
GLIBC_GROUPS: Final[tuple[tuple[str, ...], ...]] = (
    tuple(f"2.{minor}" for minor in range(5, 17)),
    tuple(f"2.{minor}" for minor in range(17, 26)),
    ("2.26",),
    ("2.27",),
    ("2.28", "2.29", "2.30"),
    ("2.31", "2.32", "2.33"),
    ("2.34",),
    ("2.35",),
    ("2.36", "2.37", "2.38"),
    ("2.39", "2.40"),
    ("2.41", "2.42"),
    ("2.43",),
)
GLIBC_REMAP: Final[dict[str, str]] = {
    glibc_version: glibc_versions[0]
    for glibc_versions in GLIBC_GROUPS
    for glibc_version in glibc_versions
}

# don't rewrite glibc_version on newer glibc, this doesn't help with
# reading the graphs without much added information
GLIBC_NSW_MAX_VERSION: Final[Version] = Version("2.31")


def _get_major_minor(x: Any) -> str:
    try:
        version = Version(x)
    except InvalidVersion:
        return "0.0"
    if version.major > 50:
        return "0.0"  # invalid version
    return f"{version.major}.{version.minor}"


def _load_df(
    wheel_support_map: dict[str, dict[str, date]],
    path: Path,
    date_: date,
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
            "python_version": _get_major_minor,
            "glibc_version": lambda x: GLIBC_REMAP.get(_get_major_minor(x), "0.0"),
            "project": lambda x: str(canonicalize_name(x)),
        },
        usecols=usecols,
    )
    df["day"] = pd.to_datetime(date_)
    # remove unneeded python version
    df.query("python_version in @PYTHON_EOL", inplace=True)
    # check if the package is supported or not for a given python version
    if "project" in usecols:

        def _get_supported_wheel(row: Any, *, src_column: str) -> str:
            value = row[src_column]
            assert isinstance(value, str)
            if src_column == "glibc_version" and Version(value) >= GLIBC_NSW_MAX_VERSION:
                return value
            project = wheel_support_map.get(row["project"])
            if project is None:
                _LOGGER.warning("%r not found in wheel_support_map", row["project"])
                return value
            max_date = project[row["python_version"]]
            if date_ < max_date:
                return value
            if row["python_version"] == "3.12":
                return f"{value}-nsw"  # no supported wheel
            return f"{value}-nsw"  # no supported wheel

        df["python_version2"] = df.apply(_get_supported_wheel, axis=1, src_column="python_version")
        df["glibc_version"] = df.apply(_get_supported_wheel, axis=1, src_column="glibc_version")

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
    _LOGGER.info("building wheel support map")
    result: dict[str, dict[str, date]] = {}
    for package in packages:
        cache_file = utils.get_release_cache_path(package)
        if not cache_file.exists():
            result[package] = dict.fromkeys(PYTHON_EOL, date.max)
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
                _LOGGER.warning('"%s": %s', package, e)
        for release, release_files in info["releases"].items():
            try:
                version_pep = Version(release)
                if version_pep.is_prerelease and not only_prerelease:
                    _LOGGER.debug('"%s": ignore pre-release %s', package, release)
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
                cp3_start = next(python for python in pythons if python.startswith("cp3"))
                start_minor = int(cp3_start[3:])
                for key in PYTHON_EOL:
                    key_minor = int(key[2:])
                    if key_minor > start_minor:
                        pythons.append(f"cp3{key_minor}")
            if any(python.startswith("py3") for python in pythons):
                py3_start = next(python for python in pythons if python.startswith("py3"))
                start_minor = int(py3_start[3:])
                for key in PYTHON_EOL:
                    key_minor = int(key[2:])
                    if key_minor > start_minor:
                        pythons.append(f"py3{key_minor}")
            supported_set = {f"{python[2]}.{python[3:]}" for python in pythons}
            for key in PYTHON_EOL:
                if key in supported_set:
                    package_result[key].supported = max(package_result[key].supported, upload_date)
                else:
                    package_result[key].not_supported.append(upload_date)
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
                        _LOGGER.warning("%r: assume python %s supported", package, key)
                    result[package][key] = date.max
                else:
                    result[package][key] = min(package_result[key].not_supported)
            else:
                package_result[key].not_supported.append(date.max)
                result[package][key] = min(
                    filter(
                        lambda x: x > package_result[key].supported,
                        package_result[key].not_supported,
                    ),
                )
            result[package][key] = previous_date = max(previous_date, result[package][key])

    removed_packages: Final[dict[str, date]] = {
        "adaptive-router-core": date(2025, 12, 21),
        "adaptive-router-core-cu12": date(2025, 12, 21),
        "ai-optix": date(2026, 2, 6),
        "aiohappyeyeball": date(2025, 10, 27),
        "archaea-core": date(2026, 1, 29),
        "baml-cc": date(2025, 12, 27),
        "baml-cc-py": date(2025, 12, 27),
        "baml-claude-code": date(2025, 12, 27),
        "bitemporal-timeseries": date(2025, 8, 20),
        "blitz-vec": date(2025, 12, 21),
        "caddy-bin-edge": date(2026, 1, 18),
        "cisco-radkit-client": date(2026, 2, 10),
        "cisco-radkit-common": date(2026, 2, 10),
        "cisco-radkit-genie": date(2026, 2, 10),
        "cisco-radkit-service": date(2026, 1, 27),
        "clogsec": date(2025, 11, 12),
        "colorinal": date(2025, 7, 27),
        "copilot-bin-edge": date(2026, 1, 18),
        "cryalg": date(2025, 9, 24),
        "dbmate-bin-edge": date(2026, 1, 18),
        "degirum-face": date(2026, 1, 21),
        "dive-bin-edge": date(2026, 1, 18),
        "entityframe": date(2025, 8, 30),
        "fastfetch-bin-edge": date(2026, 1, 18),
        "fastlap": date(2026, 2, 5),
        "feature-engineering-rs": date(2025, 11, 15),
        "flowty": date(2025, 10, 7),
        "ftp-core-bindings-pyo3": date(2026, 2, 10),
        "furiosa-model-compressor-impl": date(2026, 1, 24),
        "gamspy-conopt4": date(2025, 9, 4),
        "gamspy-sbb": date(2025, 10, 7),
        "genimtools": date(2025, 9, 19),
        "gh-bin-edge": date(2026, 1, 18),
        "grapapy": date(2025, 10, 2),  # single release, source only
        "graphbench": date(2025, 12, 2),  # single release, pure python
        "greener-reporter": date(2025, 12, 24),  # single release, pure python
        "greener-servermock": date(2025, 12, 24),
        "hikyuu-noarrow": date(2025, 12, 24),  # windows only
        "hivemind-p2p": date(2026, 1, 30),
        "ingestar": date(2026, 2, 6),
        "isagedb": date(2026, 1, 5),
        "kring": date(2025, 7, 19),
        "jasminum": date(2025, 11, 22),
        "json-repair-rust": date(2025, 11, 21),
        "just-bin-edge": date(2026, 1, 18),
        "kalign": date(2026, 2, 10),
        "lattifai-core": date(2026, 1, 21),
        "lazydocker-bin-edge": date(2026, 1, 18),
        "leanvec-edge": date(2026, 1, 26),
        "libintx-cu128": date(2025, 10, 20),
        "libmemmod": date(2025, 9, 9),
        "libp2p-pyrust": date(2025, 10, 2),
        "lib-ppca": date(2025, 9, 24),
        "lindera-py": date(2026, 1, 9),
        "llama-summarizer": date(2025, 7, 31),
        "mchp-gpio-ctl": date(2025, 10, 14),
        "minify-bin-edge": date(2026, 1, 18),
        "mosec-tiinfer": date(2025, 9, 2),
        "mrapids": date(2025, 9, 4),
        "mytorch-cnn": date(2026, 2, 10),
        "mytorch-rnn": date(2026, 2, 10),
        "nobodywhopython": date(2025, 10, 27),  # single release, windows only
        "obtest": date(2025, 9, 26),
        "ompl-genesis": date(2025, 11, 11),
        "oven-mlir": date(2025, 9, 21),
        "pairwisenamecomparator": date(2025, 11, 22),
        "perforatedai-freemium": date(2025, 9, 18),
        "pgn-extract-wg": date(2025, 9, 4),
        "phoneshift": date(2025, 9, 4),
        "pisalt": date(2025, 8, 4),
        "pivtools-cli": date(2025, 11, 11),
        "prophecy-automate": date(2026, 2, 5),
        "protectionstnd": date(2025, 12, 18),
        "pse-core": date(2025, 12, 3),
        "pybinwalk-rust": date(2025, 12, 17),
        "pycoatl": date(2025, 7, 22),
        "pydnp3-stepfunc": date(2026, 2, 7),
        "pygments-richstyle": date(2025, 11, 15),
        "pyorbbec": date(2025, 9, 15),
        "pyradiance-py313": date(2026, 2, 3),
        "pytechnicalindicators": date(2026, 1, 18),
        "python-dotenv-rs": date(2026, 2, 5),
        "qlip-algorithms": date(2025, 10, 15),
        "qlip-core": date(2025, 10, 15),
        "qlip-serve-generator": date(2025, 9, 9),
        "quasar-rs": date(2025, 12, 4),
        "rclone-bin-edge": date(2026, 1, 18),
        "repotoire": date(2026, 2, 10),
        "riyadhai-blingfire": date(2026, 1, 15),
        "rs-catch-22": date(2025, 11, 15),
        "rustmodels": date(2025, 11, 26),
        "rustivig": date(2025, 10, 8),
        "rusty-di-runner": date(2025, 12, 9),
        "scc-bin-edge": date(2026, 1, 18),
        "secval": date(2025, 12, 26),
        "sgl-kernel-cpu": date(2026, 2, 3),
        "sglang-kernel": date(2026, 2, 8),
        "silentpy": date(2025, 9, 19),  # single release, source only
        "superai-langchain": date(2026, 1, 16),  # quarantine
        "superai-llms": date(2026, 1, 20),
        "superai-ragas": date(2026, 1, 21),
        "swgo-background-library": date(2025, 12, 16),
        "synaptik-core-beta": date(2025, 10, 16),
        "tesseractpkg": date(2025, 11, 11),  # single release, pure wheel only
        "testtesttest000001": date(2025, 11, 15),
        "thestage-elastic-models": date(2025, 10, 15),
        "thestage-license": date(2025, 10, 15),
        "toondb-client": date(2026, 1, 13),
        "torcharrow": date(2025, 9, 6),
        "traefik-bin-edge": date(2026, 1, 18),
        "tzuping-algo": date(2025, 8, 28),
        "unicode-segmentation-py": date(2025, 11, 27),
        "url-handle": date(2026, 2, 1),
        "usb-monitor-utils-lib": date(2025, 9, 11),
        "usql-bin-edge": date(2026, 1, 18),
        "uuid32-utils": date(2025, 7, 25),
        "uuid64-utils": date(2025, 7, 25),
        "vatra-py": date(2025, 12, 26),
        "vc6-cuda12": date(2025, 12, 3),
        "visaionserver": date(2026, 2, 3),
        "whisper-cpp-python-smr": date(2025, 7, 31),
        "yamcot": date(2026, 2, 4),
        "zenith-ai": date(2025, 12, 18),
        "zephyr-mcumgr": date(2026, 2, 3),
        "zlgcan-driver": date(2025, 8, 4),
        "zoosyncmp": date(2026, 2, 2),
    }
    for package, removed_date in removed_packages.items():
        if package in result:
            _LOGGER.warning("%r: has been re-added", package)
            continue
        result[package] = {}
        for key in PYTHON_EOL:
            result[package][key] = removed_date

    return result


def date_iterator(start: date, end: date) -> Generator[date]:
    date_ = start
    while date_ <= end:
        yield date_
        date_ = date_ + timedelta(days=1)


def update(packages: list[str], path: Path, start: date, end: date) -> None:
    wheel_support_map = _build_wheel_support_map(packages)

    _LOGGER.info("loading data")
    with multiprocessing.Pool() as pool:
        _load_df_partial = functools.partial(_load_df, wheel_support_map, path)
        dataframes = pool.map(
            _load_df_partial,
            date_iterator(start - utils.CONSUMER_WINDOW_SIZE, end),
            chunksize=7,
        )
    dataframes = list(filter(lambda x: x is not None, dataframes))
    df = pd.concat(dataframes)

    _LOGGER.info("computing statistics")

    df = df.groupby(
        ["day", "python_version", "python_version2", "glibc_version"],
        as_index=False,
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
    df = df.stack(list(range(df.columns.nlevels)), future_stack=True).reset_index().fillna(0.0)
    df.rename(columns={0: "num_downloads"}, inplace=True)
    df = df[(df["num_downloads"] > 0) & (df["day"] >= pd.to_datetime(start))]
    df = df.groupby(
        ["day", "python_version", "python_version2", "glibc_version"],
        as_index=False,
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
        df[["glibc_version", "num_downloads"]].groupby(["day", "glibc_version"]).aggregate("sum")
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
        "last_update": datetime.now(UTC).strftime("%A, %d %B %Y, %H:%M:%S %Z"),
        "index": [d.date().isoformat() for d in df_python_all.index],
    }

    glibc_versions = [x[0] for x in GLIBC_GROUPS[::-1]]
    glibc_version = dict[str, list[str] | list[float]]()
    glibc_version["keys"] = [
        f"{v}{suffix}"
        for v in glibc_versions
        for suffix in ("", "-nsw")
        if suffix != "-nsw" or Version(v) < GLIBC_NSW_MAX_VERSION
    ]
    glibc_version_non_eol = dict[str, list[str] | list[float]]()
    glibc_version_non_eol["keys"] = glibc_version["keys"]
    for version in glibc_versions:
        for suffix in ("", "-nsw"):
            if suffix == "-nsw" and Version(version) >= GLIBC_NSW_MAX_VERSION:
                continue
            version_suffix = f"{version}{suffix}"
            stats = []
            stats_non_eol = []
            for day in out["index"]:
                try:
                    value = float(
                        df_glibc_stats.loc[(pd.to_datetime(day), version_suffix), "num_downloads"],
                    )
                except KeyError:
                    value = 0.0
                try:
                    value_non_eol = float(
                        df_glibc_non_eol_stats.loc[
                            (pd.to_datetime(day), version_suffix),
                            "num_downloads",
                        ],
                    )
                except KeyError:
                    value_non_eol = 0.0
                stats.append(float(f"{100.0 * value:.2f}"))
                stats_non_eol.append(float(f"{100.0 * value_non_eol:.2f}"))
            glibc_version[f"{version}{suffix}"] = stats
            glibc_version_non_eol[f"{version}{suffix}"] = stats_non_eol

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
                value = float(df_python_stats.loc[(pd.to_datetime(day), version), "num_downloads"])
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
                            (pd.to_datetime(day), version),
                            "num_downloads",
                        ],
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
        glibc_readiness_ver = dict[str, list[str] | list[float]]()
        glibc_readiness_ver["keys"] = cast("list[str]", list(glibc_version["keys"]))
        glibc_readiness[version] = glibc_readiness_ver
        for version_ in glibc_versions:
            for suffix in ("", "-nsw"):
                if suffix == "-nsw" and Version(version_) >= GLIBC_NSW_MAX_VERSION:
                    continue
                version_suffix = f"{version_}{suffix}"
                stats = []
                for day in out["index"]:
                    try:
                        value = float(
                            df_glibc_stats.loc[
                                (pd.to_datetime(day), version_suffix),
                                "num_downloads",
                            ],
                        )
                    except KeyError:
                        value = 0.0
                    stats.append(float(f"{100.0 * value:.2f}"))
                glibc_readiness_ver[f"{version_}{suffix}"] = stats
    # remove all zeros "-nsw" entries
    for key in list(python_version["keys"]):
        assert isinstance(key, str)
        if not key.endswith("-nsw"):
            continue
        if all(value == 0.0 for value in python_version[key]):
            python_version["keys"].remove(key)  # type: ignore[arg-type]
            python_version.pop(key)
            glibc_readiness_ver = glibc_readiness[key.rstrip("-nsw")]
            for glibc_key in list(glibc_readiness_ver["keys"]):
                assert isinstance(glibc_key, str)
                if glibc_key.endswith("-nsw"):
                    glibc_readiness_ver["keys"].remove(
                        glibc_key,  # type: ignore[arg-type]
                    )
                    glibc_readiness_ver.pop(glibc_key)

    for key in list(python_version_non_eol["keys"]):
        assert isinstance(key, str)
        if not key.endswith("-nsw"):
            continue
        if all(value == 0.0 for value in python_version_non_eol[key]):
            python_version_non_eol["keys"].remove(key)  # type: ignore[arg-type]
            python_version_non_eol.pop(key)

    out["python_version"] = python_version
    out["python_version_non_eol"] = python_version_non_eol
    out["glibc_readiness"] = glibc_readiness

    with utils.CONSUMER_DATA_PATH.open("w") as f:
        json.dump(out, f, separators=(",", ":"))

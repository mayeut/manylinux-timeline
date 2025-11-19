import dataclasses
import itertools
import json
import logging
import typing
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final, Literal

import numpy as np
import pandas as pd

import utils

_LOGGER = logging.getLogger(__name__)

POLICIES: Final[tuple[str, ...]] = (
    "ml1",
    "ml_2_5",
    "ml2010",
    "ml_2_12",
    "ml2014",
    "ml_2_17",
    "ml_2_24",
    "ml_2_26",
    "ml_2_27",
    "ml_2_28",
    "ml_2_31",
    "ml_2_34",
    "ml_2_35",
    "ml_2_36",
    "ml_2_37",
    "ml_2_38",
    "ml_2_39",
    "ml_2_40",
    "ml_2_41",
)
ArchitectureName = Literal["x86_64", "i686", "aarch64", "ppc64le", "s390x", "armv7l", "riscv64"]
ARCHITECTURES: Final[tuple[ArchitectureName, ...]] = typing.get_args(ArchitectureName)

PackageStatsName = Literal["total", "analysis"]

# python implementations are a bit more complicated...
IMPL_CP3_FIRST: Final[int] = 6
IMPL_CP3_LAST: Final[int] = 15
IMPL_PP3_FIRST: Final[int] = 8
IMPL_PP3_LAST: Final[int] = 11
# that's what is ultimately displayed
IMPLEMENTATIONS: Final[tuple[str, ...]] = tuple(
    itertools.chain(
        ["any3", "py3"],
        sorted(
            itertools.chain(
                [f"pp3{i}" for i in range(IMPL_PP3_FIRST, IMPL_PP3_LAST + 1)],
                [f"cp3{i}" for i in range(IMPL_CP3_FIRST, IMPL_CP3_LAST + 1)],
            ),
            key=lambda x: (int(x[3:]), x[:3]),
        ),
        ["abi3", "free-threaded"],
    ),
)


def _get_range_dataframe(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    for policy in POLICIES:
        df[policy] = df.manylinux.str.contains(f"{policy}_x86_64")
    for arch in ARCHITECTURES:
        df[arch] = df.manylinux.str.contains(arch)
    for version in ["abi3", "py3", "free-threaded"]:
        df[version] = df.python.str.contains(version)
    df["py32"] = df.python.str.contains("py32")
    df["cp32"] = df.python.str.contains("cp32")
    py_version_prev = "py32"
    cp_version_prev = "cp32"
    for i in range(3, IMPL_CP3_LAST + 1):
        py_version = f"py3{i}"
        cp_version = f"cp3{i}"
        df[py_version] = df.python.str.contains(py_version) | df[py_version_prev]
        df[cp_version] = (
            df.python.str.contains(cp_version) | df[py_version] | (df["abi3"] & df[cp_version_prev])
        )
        py_version_prev = py_version
        cp_version_prev = cp_version
    for i in range(IMPL_PP3_FIRST, IMPL_PP3_LAST + 1):
        py_version = f"py3{i}"
        pp_version = f"pp3{i}"
        df[pp_version] = df.python.str.contains(pp_version) | df[py_version]
    df["any3"] = (
        df.python.str.contains("py3")
        | df.python.str.contains("cp3")
        | df.python.str.contains("pp3")
    )
    df_r = df[(df["day"] >= (start - utils.PRODUCER_WINDOW_SIZE)) & (df["day"] < end)]
    df_r = df_r.drop(columns=["version", "python", "manylinux"])
    return df_r.sort_values("day", ascending=False).copy(deep=True)


def _get_rolling_dataframe(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> tuple[list[str], pd.DataFrame]:
    current = end_date
    step = timedelta(days=1)
    index = []
    rolling_dfs = []
    while current >= start_date:
        window_start = current - utils.PRODUCER_WINDOW_SIZE
        df_window = (
            df[(df["day"] >= window_start) & (df["day"] < current)]
            .drop_duplicates(["package"])
            .drop(columns=["package"])
        )
        df_window["day"] = current
        rolling_dfs.append(df_window)
        index.append(current)
        current -= step
    index_as_str = [d.date().isoformat() for d in index[::-1]]
    return index_as_str, pd.concat(rolling_dfs).sort_values("day")


def _get_stats_df(full_dataframe: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    values = full_dataframe.value_counts(subset=["day", *columns], sort=False)
    df_with_count = values.unstack(columns, fill_value=0.0)
    return df_with_count.apply(lambda x: x / np.sum(x), axis=1)


def _get_stats(df: pd.DataFrame, key: Iterable[bool], level: Iterable[str]) -> list[float]:
    ts = df.xs(key=tuple(key), axis=1, level=level).apply(np.sum, axis=1)
    ts.index = pd.DatetimeIndex(ts.index.get_level_values(0).values, name="day")
    return [float(f"{100.0 * value:.2f}") for value in ts.sort_index().values]


def _get_total_packages(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> list[int]:
    ts = df.sort_values("day").drop_duplicates("package").value_counts(subset=["day"], sort=False)
    ts.index = pd.DatetimeIndex(ts.index.get_level_values(0).values, name="day")
    offset = timedelta(days=1)
    stop = max(pd.to_datetime(ts.index.values[-1]), end_date) + offset
    ts[stop] = 0
    ts = ts.cumsum().resample("1d").ffill()
    ts.index += offset
    ts = ts[(ts.index >= start_date) & (ts.index <= end_date)]
    return [int(value) for value in ts.sort_index().values]


@dataclasses.dataclass
class ProducerStats:
    last_update: str = datetime.now(UTC).strftime("%A, %d %B %Y, %H:%M:%S %Z")
    package_count: int = 0
    index: list[str] = dataclasses.field(default_factory=list)
    lowest_policy: dict[str, list[float]] = dataclasses.field(default_factory=dict)
    highest_policy: dict[str, list[float]] = dataclasses.field(default_factory=dict)
    implementation: dict[str, list[float]] = dataclasses.field(default_factory=dict)
    architecture: dict[ArchitectureName, list[float]] = dataclasses.field(default_factory=dict)
    package: dict[PackageStatsName, list[int]] = dataclasses.field(default_factory=dict)

    def to_json(self, path: Path) -> None:
        assert len(self.lowest_policy) == len(POLICIES)
        assert all(len(value) == len(self.index) for value in self.lowest_policy.values())
        assert self.highest_policy.keys() == self.lowest_policy.keys()
        assert all(len(value) == len(self.index) for value in self.highest_policy.values())
        assert self.implementation.keys() == set(IMPLEMENTATIONS)
        assert all(len(value) == len(self.index) for value in self.implementation.values())
        assert self.architecture.keys() == set(ARCHITECTURES)
        assert all(len(value) == len(self.index) for value in self.architecture.values())
        assert len(self.package) == len(typing.get_args(PackageStatsName))
        assert all(len(value) == len(self.index) for value in self.package.values())
        with path.open("w") as f:
            json.dump(
                {
                    "last_update": self.last_update,
                    "package_count": self.package_count,
                    "index": self.index,
                    "lowest_policy": {
                        "keys": list(self.lowest_policy.keys()),
                        **self.lowest_policy,
                    },
                    "highest_policy": {
                        "keys": list(self.highest_policy.keys()),
                        **self.highest_policy,
                    },
                    "implementation": {
                        "keys": list(self.implementation.keys()),
                        **self.implementation,
                    },
                    "architecture": {"keys": list(self.architecture.keys()), **self.architecture},
                    "package": {"keys": list(self.package.keys()), **self.package},
                },
                f,
                separators=(",", ":"),
            )


def update(rows: Iterable[utils.Row], start: date, end: date) -> None:
    out = ProducerStats()
    pd.set_option("display.max_columns", None)
    end_date = pd.to_datetime(end)  # start at end
    start_date = pd.to_datetime(start)
    _LOGGER.info("create main data frame")
    df = pd.DataFrame.from_records(rows, columns=utils.Row._fields)
    df["day"] = pd.to_datetime(df["day"])
    out.package["total"] = _get_total_packages(df, start_date, end_date)
    df = _get_range_dataframe(df, start_date, end_date)
    out.package_count = int(df[["package"]].drop_duplicates().agg("count")["package"])
    _LOGGER.info("update dataframe using a %d days sliding window", utils.PRODUCER_WINDOW_SIZE.days)
    out.index, rolling_df = _get_rolling_dataframe(df, start_date, end_date)

    _LOGGER.info("compute statistics")
    ts = rolling_df.value_counts(subset=["day"], sort=False)
    ts.index = pd.DatetimeIndex(ts.index.get_level_values(0).values, name="day")
    out.package["analysis"] = ts.sort_index().values.tolist()
    policy_df = _get_stats_df(rolling_df[rolling_df["x86_64"]], POLICIES)
    len_ = len(POLICIES)
    for i in range(len_):
        name = POLICIES[i].replace("ml", "manylinux")
        out.highest_policy[name] = _get_stats(
            policy_df,
            key=[True] + [False] * (len_ - i - 1),
            level=POLICIES[i:],
        )
        out.lowest_policy[name] = _get_stats(
            policy_df,
            key=[False] * i + [True],
            level=POLICIES[: i + 1],
        )

    arch_df = _get_stats_df(rolling_df, ARCHITECTURES)
    for arch in ARCHITECTURES:
        out.architecture[arch] = _get_stats(arch_df, key=[True], level=[arch])

    impl_df = _get_stats_df(rolling_df, IMPLEMENTATIONS)
    for impl in IMPLEMENTATIONS:
        out.implementation[impl] = _get_stats(impl_df, key=[True], level=[impl])

    out.to_json(utils.PRODUCER_DATA_PATH)

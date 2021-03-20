import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Union

import numpy as np
import pandas as pd
from packaging.version import Version

import utils

POLICIES = {
    0: "none",
    1: "manylinux1",
    2: "manylinux2010",
    3: "manylinux2014",
    4: "manylinux_2_17",
    5: "manylinux_2_19",
    6: "manylinux_2_23",
    7: "manylinux_2_24",
    8: "manylinux_2_26",
    9: "manylinux_2_27",
    10: "manylinux_2_28",
    11: "manylinux_2_31",
}


def _get_major_minor(x):
    version = Version(x)
    return f"{version.major}.{version.minor}"


def _load_df(path: Path, date: datetime) -> Optional[pd.DataFrame]:
    folder = path / date.strftime("%Y") / date.strftime("%m")
    file = folder / f"{date.strftime('%d')}.csv"
    if not file.exists():
        return None
    df = pd.read_csv(
        file,
        converters={
            "python_version": lambda x: _get_major_minor(x),
            "pip_version": lambda x: _get_major_minor(x),
            "glibc_version": lambda x: _get_major_minor(x),
        },
    )
    df["day"] = pd.to_datetime(date)
    return df


def update(path: Path, start: datetime, end: datetime):
    date_ = start
    dataframes = []
    while date_ < end:
        df = _load_df(path, date_)
        if df is not None:
            dataframes.append(df)
        date_ = date_ + timedelta(days=1)

    df = pd.concat(dataframes)
    df_glibc = (
        df[["day", "glibc_version", "num_downloads"]]
        .groupby(["day", "glibc_version"])
        .aggregate(np.sum)
    )
    df_glibc_all = df_glibc.groupby(["day"]).aggregate(np.sum)
    df_glibc_stats = df_glibc / df_glibc_all
    pip_version = df["pip_version"].str.split(".", n=2, expand=True)
    df["pip_major"] = pd.to_numeric(pip_version[0])
    df["pip_minor"] = pd.to_numeric(pip_version[1])
    glibc_version = df["glibc_version"].str.split(".", n=2, expand=True)
    df["glibc_major"] = pd.to_numeric(glibc_version[0])
    df["glibc_minor"] = pd.to_numeric(glibc_version[1])
    df["manylinux1"] = (
        ((df.pip_major > 8) | ((df.pip_major == 8) & (df.pip_minor >= 1)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 5)))
    ).astype(int)
    df["manylinux2010"] = (
        ((df.pip_major > 19) | ((df.pip_major == 19) & (df.pip_minor >= 0)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 12)))
    ).astype(int)
    df["manylinux2014"] = (
        ((df.pip_major > 19) | ((df.pip_major == 19) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 17)))
    ).astype(int)
    df["manylinux_2_17"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 17)))
    ).astype(int)
    df["manylinux_2_19"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 19)))
    ).astype(int)
    df["manylinux_2_23"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 23)))
    ).astype(int)
    df["manylinux_2_24"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 24)))
    ).astype(int)
    df["manylinux_2_26"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 26)))
    ).astype(int)
    df["manylinux_2_27"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 27)))
    ).astype(int)
    df["manylinux_2_28"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 28)))
    ).astype(int)
    df["manylinux_2_31"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 31)))
    ).astype(int)
    df["policy"] = (
        df["manylinux1"]
        + df["manylinux2010"]
        + df["manylinux2014"]
        + df["manylinux_2_17"]
        + df["manylinux_2_19"]
        + df["manylinux_2_23"]
        + df["manylinux_2_24"]
        + df["manylinux_2_26"]
        + df["manylinux_2_27"]
        + df["manylinux_2_28"]
        + df["manylinux_2_31"]
    )
    df.drop(
        columns=[
            "pip_version",
            "glibc_version",
            "pip_major",
            "pip_minor",
            "glibc_major",
            "glibc_minor",
            "manylinux1",
            "manylinux2010",
            "manylinux2014",
            "manylinux_2_17",
            "manylinux_2_19",
            "manylinux_2_23",
            "manylinux_2_24",
            "manylinux_2_26",
            "manylinux_2_27",
            "manylinux_2_28",
            "manylinux_2_31",
        ],
        inplace=True,
    )
    df = df[(df["cpu"] == "x86_64") | (df["cpu"] == "i686")]
    df.drop(columns=["cpu"], inplace=True)
    df = df.groupby(["day", "python_version", "policy"], as_index=False).aggregate(
        np.sum
    )
    df.set_index("day", append=True, inplace=True)
    df = df.swaplevel()

    # python version download stats
    df_python = df.drop(columns=["policy"])
    df_python = df_python.groupby(["day", "python_version"]).aggregate(np.sum)
    df_python_all = df_python.groupby(["day"]).aggregate(np.sum)
    df_python_stats = df_python / df_python_all

    out: dict[str, Any] = {
        "last_update": datetime.now(timezone.utc).strftime("%A, %d %B %Y, %H:%M:%S %Z"),
        "index": list([d.date().isoformat() for d in df_python_all.index]),
    }

    # combine some versions to remove some of the less used ones
    # but still accounting for the smaller one
    glibc_versions = [
        ("2.5", "2.6"),
        ("2.7", "2.8"),
        ("2.9", "2.10", "2.11"),
        ("2.12", "2.13", "2.14", "2.15", "2.16"),
        ("2.17", "2.18"),
        ("2.19", "2.20", "2.21", "2.22"),
        ("2.23",),
        ("2.24", "2.25"),
        ("2.26",),
        ("2.27",),
        ("2.28", "2.29", "2.30"),
    ] + list([(f"2.{minor}",) for minor in range(31, 34)])
    glibc_versions = glibc_versions[::-1]
    glibc_version = dict[str, Union[list[str], list[float]]]()
    glibc_version["keys"] = list([v[0] for v in glibc_versions])
    for versions in glibc_versions:
        stats = []
        for day in out["index"]:
            value = 0.0
            for version in versions:
                try:
                    value += float(
                        df_glibc_stats.loc[
                            (pd.to_datetime(day), version), "num_downloads"
                        ]
                    )
                except KeyError:
                    pass
            stats.append(float(f"{100.0 * value:.2f}"))
        glibc_version[versions[0]] = stats
    out["glibc_version"] = glibc_version

    python_versions = ["2.7", "3.5", "3.6", "3.7", "3.8", "3.9"]
    python_version = dict[str, Union[list[str], list[float]]]()
    policy_readiness = dict[str, dict[str, Union[list[str], list[float]]]]()
    python_version["keys"] = python_versions
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

        df_policy = df[df["python_version"] == version]
        df_policy = df_policy.drop(columns=["python_version"])
        df_policy = df_policy.groupby(["day", "policy"]).aggregate(np.sum)
        df_policy_all = df_policy.groupby(["day"]).aggregate(np.sum)
        df_policy_stats = df_policy / df_policy_all
        policy_readiness_ver = dict[str, Union[list[str], list[float]]]()
        policy_readiness_ver["keys"] = list(
            [POLICIES[i] for i in range(len(POLICIES))[::-1]]
        )
        policy_readiness[version] = policy_readiness_ver
        for i in range(len(POLICIES))[::-1]:
            policy = POLICIES[i]
            stats = []
            for day in out["index"]:
                try:
                    value = float(
                        df_policy_stats.loc[(pd.to_datetime(day), i), "num_downloads"]
                    )
                except KeyError:
                    value = 0.0
                stats.append(float(f"{100.0 * value:.2f}"))
            policy_readiness_ver[policy] = stats
    out["python_version"] = python_version
    out["policy_readiness"] = policy_readiness

    with open(utils.CONSUMER_DATA_PATH, "w") as f:
        json.dump(out, f, separators=(",", ":"))

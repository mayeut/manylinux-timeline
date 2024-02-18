import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Union

import pandas as pd
from packaging.version import InvalidVersion, Version

import utils

POLICIES = {
    0: "none",
    1: "manylinux1",
    2: "manylinux2010",
    3: "manylinux2014",
    4: "manylinux_2_17",
    5: "manylinux_2_23",
    6: "manylinux_2_24",
    7: "manylinux_2_26",
    8: "manylinux_2_27",
    9: "manylinux_2_28",
    10: "manylinux_2_31",
    11: "manylinux_2_34",
    12: "manylinux_2_35",
}

PYTHON_EOL = {
    "2.7": pd.to_datetime("2020-01-01"),
    "3.5": pd.to_datetime("2020-09-13"),
    "3.6": pd.to_datetime("2021-12-23"),
    "3.7": pd.to_datetime("2023-06-27"),
    "3.8": pd.to_datetime("2024-10-14"),
    "3.9": pd.to_datetime("2025-10-05"),
    "3.10": pd.to_datetime("2026-10-04"),
    "3.11": pd.to_datetime("2027-10-24"),
    "3.12": pd.to_datetime("2028-10-04"),
    "3.13": pd.to_datetime("2029-10-01"),
}


def _get_major_minor(x):
    try:
        version = Version(x)
    except InvalidVersion:
        return "0.0"
    if version.major > 50:
        return "0.0"  # invalid version
    return f"{version.major}.{version.minor}"


def _load_df(path: Path, date: datetime) -> pd.DataFrame | None:
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
    date_ = start - utils.CONSUMER_WINDOW_SIZE
    dataframes = []
    while date_ < end:
        df = _load_df(path, date_)
        if df is not None:
            dataframes.append(df)
        date_ = date_ + timedelta(days=1)

    df = pd.concat(dataframes)

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
    df["manylinux_2_34"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 34)))
    ).astype(int)
    df["manylinux_2_35"] = (
        ((df.pip_major > 20) | ((df.pip_major == 20) & (df.pip_minor >= 3)))
        & ((df.glibc_major > 2) | ((df.glibc_major == 2) & (df.glibc_minor >= 35)))
    ).astype(int)
    df["policy"] = (
        df["manylinux1"]
        + df["manylinux2010"]
        + df["manylinux2014"]
        + df["manylinux_2_17"]
        + df["manylinux_2_23"]
        + df["manylinux_2_24"]
        + df["manylinux_2_26"]
        + df["manylinux_2_27"]
        + df["manylinux_2_28"]
        + df["manylinux_2_31"]
        + df["manylinux_2_34"]
        + df["manylinux_2_35"]
    )
    df.drop(
        columns=[
            "pip_version",
            "pip_major",
            "pip_minor",
            "glibc_major",
            "glibc_minor",
            "manylinux1",
            "manylinux2010",
            "manylinux2014",
            "manylinux_2_17",
            "manylinux_2_23",
            "manylinux_2_24",
            "manylinux_2_26",
            "manylinux_2_27",
            "manylinux_2_28",
            "manylinux_2_31",
            "manylinux_2_34",
            "manylinux_2_35",
        ],
        inplace=True,
    )
    df = df[(df["cpu"] == "x86_64") | (df["cpu"] == "i686")]
    df.drop(columns=["cpu"], inplace=True)
    df = df.groupby(
        ["day", "python_version", "glibc_version", "policy"], as_index=False
    ).aggregate("sum")

    # apply rolling window
    df = pd.pivot_table(
        df,
        index="day",
        columns=["python_version", "glibc_version", "policy"],
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
        ["day", "python_version", "glibc_version", "policy"], as_index=False
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
        df[["python_version", "num_downloads"]]
        .groupby(["day", "python_version"])
        .aggregate("sum")
    )
    df_python_all = df_python.groupby(["day"]).aggregate("sum")
    df_python_stats = df_python / df_python_all

    df_python_non_eol = (
        df_non_eol[["python_version", "num_downloads"]]
        .groupby(["day", "python_version"])
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
        ("2.5", "2.6", "2.7", "2.8", "2.9", "2.10", "2.11"),
        ("2.12", "2.13", "2.14", "2.15", "2.16"),
        ("2.17", "2.18", "2.19", "2.20", "2.21", "2.22"),
        ("2.23",),
        ("2.24", "2.25"),
        ("2.26",),
        ("2.27",),
        ("2.28", "2.29", "2.30"),
        ("2.31", "2.32", "2.33"),
        ("2.34",),
        ("2.35", "2.36", "2.37", "2.38"),
    ]
    glibc_versions = glibc_versions[::-1]
    glibc_version = dict[str, Union[list[str], list[float]]]()
    glibc_version["keys"] = list(v[0] for v in glibc_versions)
    glibc_version_non_eol = dict[str, Union[list[str], list[float]]]()
    glibc_version_non_eol["keys"] = list(v[0] for v in glibc_versions)
    for versions in glibc_versions:
        stats = []
        stats_non_eol = []
        for day in out["index"]:
            value = 0.0
            value_non_eol = 0.0
            for version in versions:
                try:
                    value += float(
                        df_glibc_stats.loc[
                            (pd.to_datetime(day), version), "num_downloads"
                        ]
                    )
                except KeyError:
                    pass
                try:
                    value_non_eol += float(
                        df_glibc_non_eol_stats.loc[
                            (pd.to_datetime(day), version), "num_downloads"
                        ]
                    )
                except KeyError:
                    pass
            stats.append(float(f"{100.0 * value:.2f}"))
            stats_non_eol.append(float(f"{100.0 * value_non_eol:.2f}"))
        glibc_version[versions[0]] = stats
        glibc_version_non_eol[versions[0]] = stats_non_eol

    out["glibc_version"] = glibc_version
    out["glibc_version_non_eol"] = glibc_version_non_eol

    python_versions_no_pep600_pip = ["2.7", "3.5", "3.6", "3.7", "3.8", "3.9"]
    python_versions = python_versions_no_pep600_pip + [
        v for v in PYTHON_EOL if v not in python_versions_no_pep600_pip
    ]
    python_version = dict[str, list[str] | list[float]]()
    python_version_non_eol = dict[str, list[str] | list[float]]()
    policy_readiness = dict[str, dict[str, list[str] | list[float]]]()
    glibc_readiness = dict[str, dict[str, list[str] | list[float]]]()
    python_version["keys"] = python_versions
    python_version_non_eol["keys"] = [
        version
        for version in python_versions
        if PYTHON_EOL[version] > pd.to_datetime(start)
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

        df_python_version = df[df["python_version"] == version]

        if version in python_versions_no_pep600_pip:
            df_policy = (
                df_python_version[["policy", "num_downloads"]]
                .groupby(["day", "policy"])
                .aggregate("sum")
            )
            df_policy_all = df_policy.groupby(["day"]).aggregate("sum")
            df_policy_stats = df_policy / df_policy_all
            policy_readiness_ver = dict[str, Union[list[str], list[float]]]()
            policy_readiness_ver["keys"] = list(
                POLICIES[i] for i in range(len(POLICIES))[::-1]
            )
            policy_readiness[version] = policy_readiness_ver
            for i in range(len(POLICIES))[::-1]:
                policy = POLICIES[i]
                stats = []
                for day in out["index"]:
                    try:
                        value = float(
                            df_policy_stats.loc[
                                (pd.to_datetime(day), i), "num_downloads"
                            ]
                        )
                    except KeyError:
                        value = 0.0
                    stats.append(float(f"{100.0 * value:.2f}"))
                policy_readiness_ver[policy] = stats

        df_glibc = (
            df_python_version[["glibc_version", "num_downloads"]]
            .groupby(["day", "glibc_version"])
            .aggregate("sum")
        )
        df_glibc_all = df_glibc.groupby(["day"]).aggregate("sum")
        df_glibc_stats = df_glibc / df_glibc_all
        glibc_readiness_ver = dict[str, Union[list[str], list[float]]]()
        glibc_readiness_ver["keys"] = list(v[0] for v in glibc_versions)
        glibc_readiness[version] = glibc_readiness_ver
        for versions in glibc_versions:
            stats = []
            for day in out["index"]:
                value = 0.0
                for glibc_version in versions:
                    try:
                        value += float(
                            df_glibc_stats.loc[
                                (pd.to_datetime(day), glibc_version), "num_downloads"
                            ]
                        )
                    except KeyError:
                        pass
                stats.append(float(f"{100.0 * value:.2f}"))
            glibc_readiness_ver[versions[0]] = stats

    out["python_version"] = python_version
    out["python_version_non_eol"] = python_version_non_eol
    out["policy_readiness"] = policy_readiness
    out["glibc_readiness"] = glibc_readiness

    with open(utils.CONSUMER_DATA_PATH, "w") as f:
        json.dump(out, f, separators=(",", ":"))

import argparse
import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
from shutil import copy, rmtree

import update_cache
import update_consumer_data
import update_consumer_stats
import update_dataset
import update_stats
import utils

_LOGGER = logging.getLogger(__name__)


def check_file(value):
    if value is None:
        return None
    result = Path(value)
    if not result.exists() or not result.is_file():
        raise ValueError(result)
    return result


def main() -> None:
    default_end = date.today() - timedelta(days=1)
    default_start = default_end - timedelta(days=365 * 2)

    parser = argparse.ArgumentParser(
        description="Update manylinux timeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--all-pypi-packages",
        action="store_true",
        help="check all packages in PyPI.",
    )
    parser.add_argument(
        "--update-filters",
        action="store_true",
        help="check minimum python version for each of the top 8000 PyPI packages.",
    )
    parser.add_argument(
        "-s",
        "--start",
        default=default_start,
        type=date.fromisoformat,
        help="start date",
    )
    parser.add_argument(
        "-e", "--end", default=default_end, type=date.fromisoformat, help="end date"
    )
    parser.add_argument("--skip-cache", action="store_true", help="skip cache update")
    parser.add_argument(
        "--bigquery-credentials",
        type=check_file,
        help="path to bigquery credentials (enables bigquery)",
    )
    parser.add_argument(
        "-v", "--verbosity", action="count", help="increase output verbosity"
    )
    args = parser.parse_args()

    logging.basicConfig(level=30 - 10 * min(args.verbosity or 0, 2))
    start = args.start
    end = args.end
    if end > default_end:
        end = default_end
        _LOGGER.warning(
            f"end date ({args.end}) adjusted to the default end " f"date ({end})"
        )
    if start >= end:
        raise ValueError(f"{start} >= {end}")

    if "GITHUB_EVENT_NAME" in os.environ:
        event_name = os.environ["GITHUB_EVENT_NAME"]
        today = date.today()
        if event_name == "schedule" and today.isoweekday() == 1:
            # check every PyPI packages every Monday
            args.all_pypi_packages = True

    if utils.BUILD_PATH.exists():
        rmtree(utils.BUILD_PATH)
    utils.BUILD_PATH.mkdir()
    utils.CACHE_PATH.mkdir(exist_ok=True)

    _LOGGER.debug("loading package list")
    with open(utils.ROOT_PATH / "packages.json") as f:
        packages: list[str] = json.load(f)
    _LOGGER.debug(f"loaded {len(packages)} package names")

    _LOGGER.debug("updating consumer data")
    update_consumer_data.update(
        packages, utils.ROOT_PATH / "consumer_data", args.bigquery_credentials
    )
    update_consumer_stats.update(
        packages, utils.ROOT_PATH / "consumer_data", start, end
    )

    if not args.skip_cache:
        packages = update_cache.update(packages, args.all_pypi_packages)

    packages, rows = update_dataset.update(packages)
    with open(utils.ROOT_PATH / "packages.json", "w") as f:
        json.dump(packages, f, indent=0)
        f.write("\n")
    update_stats.update(rows, start, end)
    copy(utils.ROOT_PATH / "index.html", utils.BUILD_PATH)
    copy(utils.ROOT_PATH / "style.css", utils.BUILD_PATH)
    copy(utils.ROOT_PATH / "favicon.ico", utils.BUILD_PATH)
    copy(utils.ROOT_PATH / ".gitignore", utils.BUILD_PATH)


if __name__ == "__main__":
    main()

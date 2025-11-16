import re
from datetime import date, timedelta
from pathlib import Path
from typing import NamedTuple

ROOT_PATH = Path(__file__).resolve().parent
BUILD_PATH = ROOT_PATH / "build"
PRODUCER_DATA_PATH = BUILD_PATH / "producer-data.json"
CONSUMER_DATA_PATH = BUILD_PATH / "consumer-data.json"
CACHE_PATH = ROOT_PATH / "cache"
RELEASE_INFO_PATH = CACHE_PATH / "info"
PRODUCER_WINDOW_SIZE = timedelta(days=182)
CONSUMER_WINDOW_SIZE = timedelta(days=28)
USER_AGENT = "manylinux-timeline/1.0 (https://github.com/mayeut/manylinux-timeline)"


class Row(NamedTuple):
    day: date
    package: str
    version: str
    python: str
    manylinux: str


WHEEL_INFO_RE = re.compile(
    r"""^(?P<namever>(?P<name>.+?)-(?P<ver>.+?))(?:-(?P<build>\d[^-]*))?
     -(?P<pyver>.+?)-(?P<abi>.+?)-(?P<plat>.+?)\.whl$""",
    re.VERBOSE,
)


class WheelMetadata(NamedTuple):
    name: str
    version: str
    build_tag: str | None
    implementation: str
    abi: str
    platform: str


def get_release_cache_path(package: str) -> Path:
    return RELEASE_INFO_PATH / f"{package}.json"

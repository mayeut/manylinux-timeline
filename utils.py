import re

from datetime import date, timedelta
from pathlib import Path
from typing import NamedTuple, Optional

ROOT_PATH = Path(__file__).resolve().parent
BUILD_PATH = ROOT_PATH / 'build'
DATA_PATH = BUILD_PATH / 'data.json'
CACHE_PATH = ROOT_PATH / 'cache'
RELEASE_INFO_PATH = CACHE_PATH / 'info'
WEEK_DELTA = timedelta(days=7)
WINDOW_SIZE = WEEK_DELTA * 26
USER_AGENT = ('manylinux-timeline/1.0 '
              '(https://github.com/mayeut/manylinux-timeline)')

Row = NamedTuple('Row', [
    ('day', date),
    ('package', str),
    ('version', str),
    ('python', str),
    ('manylinux', str)
])

WHEEL_INFO_RE = re.compile(
    r"""^(?P<namever>(?P<name>.+?)-(?P<ver>.+?))(?:-(?P<build>\d[^-]*))?
     -(?P<pyver>.+?)-(?P<abi>.+?)-(?P<plat>.+?)\.whl$""",
    re.VERBOSE)

WheelMetadata = NamedTuple('_WheelMetadata', [
    ('name', str),
    ('version', str),
    ('build_tag', Optional[str]),
    ('implementation', str),
    ('abi', str),
    ('platform', str)
])


def week_start(date_):
    return date.fromisocalendar(*date_.isocalendar()[:2], 1)


def get_release_cache_path(package):
    return RELEASE_INFO_PATH / f'{package}.json'

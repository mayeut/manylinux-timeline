from datetime import date, timedelta
from pathlib import Path
from typing import NamedTuple

ROOT_PATH = Path(__file__).resolve().parent
BUILD_PATH = ROOT_PATH / 'build'
CACHE_PATH = ROOT_PATH / 'rows.csv.xz'
DATA_PATH = BUILD_PATH / 'data.json'
WEEK_DELTA = timedelta(days=7)
WINDOW_SIZE = WEEK_DELTA * 26

Row = NamedTuple('Row', [
    ('week', str),
    ('package', str),
    ('version', str),
    ('python', str),
    ('manylinux', str)
])


def week_start(date_):
    return date.fromisocalendar(*date_.isocalendar()[:2], 1)


def from_week_str(week_str):
    year, week = week_str.split('-')
    return date.fromisocalendar(int(year), int(week), 1)


def to_week_str(date_):
    year, week, _ = date_.isocalendar()
    return f'{year:04d}-{week:02d}'

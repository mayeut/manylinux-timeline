import argparse
import logging

from datetime import date
from shutil import copy, rmtree

import update_cache
import update_stats
import utils


_LOGGER = logging.getLogger(__name__)


if __name__ == "__main__":
    default_end = utils.week_start(date.today()) - utils.WEEK_DELTA
    default_start = default_end - 104 * utils.WEEK_DELTA

    parser = argparse.ArgumentParser(
        description='Update manylinux timeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-t', '--top-packages', action='store_true',
        help='check for new packages using manylinux wheels in top packages'
    )
    parser.add_argument('-s', '--start', default=default_start,
                        type=date.fromisoformat, help='start date')
    parser.add_argument('-e', '--end', default=default_end,
                        type=date.fromisoformat, help='end date')
    parser.add_argument('-v', '--verbosity', action='count',
                        help='increase output verbosity')
    args = parser.parse_args()

    logging.basicConfig(level=30 - 10 * min(args.verbosity or 0, 2))
    start = utils.week_start(args.start)
    if start != args.start:
        _LOGGER.warning(f'start date ({args.start}) adjusted to 1st day of the '
                        f'same week ({start}) ')
    end = utils.week_start(args.end)
    if end != args.end:
        _LOGGER.warning(f'end date ({args.end}) adjusted to 1st day of the '
                        f'same week ({end}) ')

    if end > default_end:
        end = default_end
        _LOGGER.warning(f'end date ({args.end}) adjusted to the default end '
                        f'date ({end}) to let packagers time to release all '
                        'manylinux wheels')

    if start >= end:
        raise ValueError(f'{start} >= {end}')
    if utils.BUILD_PATH.exists():
        rmtree(utils.BUILD_PATH)
    utils.BUILD_PATH.mkdir()
    utils.CACHE_PATH.mkdir(exist_ok=True)
    update_cache.update(start, end, args.top_packages)
    update_stats.update(start, end)
    copy(utils.ROOT_PATH / 'index.html', utils.BUILD_PATH)
    copy(utils.ROOT_PATH / 'style.css', utils.BUILD_PATH)
    copy(utils.ROOT_PATH / 'favicon.ico', utils.BUILD_PATH)

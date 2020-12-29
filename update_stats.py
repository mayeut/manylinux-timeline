import itertools
import json
import logging

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import utils


_LOGGER = logging.getLogger(__name__)

POLICIES = ('ml1', 'ml2010', 'ml2014')
ARCHITECTURES = ('x86_64', 'i686', 'aarch64', 'ppc64le', 's390x')
# python implementations are a bit more complicated...
IMPL_X2 = ('cp27', 'pp27')
IMPL_CP3_FIRST = 5
IMPL_CP3_LAST = 10
IMPL_PP3 = tuple(f'pp3{i}' for i in range(6, 7 + 1))
# that's what is ultimately displayed
IMPLEMENTATIONS = tuple(itertools.chain(
    ['any2', 'py2'],
    IMPL_X2,
    ['any3', 'py3'],
    sorted(itertools.chain(
        IMPL_PP3,
        [f'cp3{i}' for i in range(IMPL_CP3_FIRST, IMPL_CP3_LAST + 1)]
    ), key=lambda x: (int(x[3:]), x[:3])),
    ['abi3']
))


def _get_full_dataframe(rows, start, end):
    df = pd.DataFrame.from_records(rows, columns=utils.Row._fields)
    for policy in POLICIES:
        df[policy] = df.manylinux.str.contains(f'{policy}_x86_64')
    for arch in ARCHITECTURES:
        df[arch] = df.manylinux.str.contains(arch)
    for version in itertools.chain(IMPL_X2, IMPL_PP3, ['py2', 'py3', 'abi3']):
        df[version] = df.python.str.contains(version)
    df['cp32'] = df.python.str.contains('cp32')
    for i in range(3, IMPL_CP3_LAST + 1):
        version = f'cp3{i}'
        version_prev = f'cp3{i - 1}'
        df[version] = df.python.str.contains(version) | \
            (df['abi3'] & df[f'{version_prev}'])
    df['any2'] = df.python.str.contains('py2') | df.python.str.contains('cp2') \
        | df.python.str.contains('pp2')
    df['any3'] = df.python.str.contains('py3') | df.python.str.contains('cp3') \
        | df.python.str.contains('pp3')
    df_r = df[(df['day'] >= (start - utils.WINDOW_SIZE)) & (df['day'] < end)]
    df_r = df_r.drop(columns=['version', 'python', 'manylinux'])
    return df_r.sort_values('day', ascending=False).copy(deep=True)


def _get_rolling_dataframe(df, start_date, end_date):
    current = end_date
    step = timedelta(days=1)
    index = []
    rolling_dfs = []
    while current >= start_date:
        window_start = current - utils.WINDOW_SIZE
        df_window = df[(df['day'] >= window_start) & (df['day'] < current)].\
            drop_duplicates(['package']).drop(columns=['package'])
        df_window['day'] = current
        rolling_dfs.append(df_window)
        index.append(current)
        current -= step
    index_as_str = list([d.date().isoformat() for d in index[::-1]])
    return index_as_str, pd.concat(rolling_dfs).sort_values('day')


def _get_stats_df(full_dataframe, columns):
    columns_ = list(columns)
    values = full_dataframe.value_counts(subset=['day'] + columns_, sort=False)
    df_with_count = values.unstack(columns_, fill_value=0.0)
    return df_with_count.apply(lambda x: x / np.sum(x), axis=1)


def _get_stats(df, key, level):
    values = df.xs(key=key, axis=1, level=level).apply(np.sum, axis=1).values
    return [float(f'{100.0 * value:.1f}') for value in values]


def update(rows, start, end):
    out = {
        'last_update': datetime.now(timezone.utc).strftime(
            '%A, %d %B %Y, %H:%M:%S %Z'),
        'package_count': 0,
        'index': [],
        'lowest_policy': {},
        'highest_policy': {},
        'implementation': {},
        'architecture': {},
    }
    pd.set_option('display.max_columns', None)
    end_date = pd.to_datetime(end)  # start at end
    start_date = pd.to_datetime(start)
    _LOGGER.info('create main data frame')
    df = _get_full_dataframe(rows, start_date, end_date)
    out['package_count'] = int(
        df[['package']].drop_duplicates().agg('count')['package'])
    _LOGGER.info(f'update dataframe using a {utils.WINDOW_SIZE.days} days '
                 'sliding window')
    out['index'], rolling_df = _get_rolling_dataframe(df, start_date, end_date)

    _LOGGER.info('compute statistics')
    policy_df = _get_stats_df(rolling_df[rolling_df['x86_64']], POLICIES)
    len_ = len(POLICIES)
    out['highest_policy']['keys'] = []
    out['lowest_policy']['keys'] = []
    for i in range(len_):
        name = POLICIES[i].replace('ml', 'manylinux')
        out['highest_policy']['keys'].append(name)
        out['highest_policy'][name] = _get_stats(
            policy_df, key=[True] + [False] * (len_ - i - 1),
            level=POLICIES[i:])
        out['lowest_policy']['keys'].append(name)
        out['lowest_policy'][name] = _get_stats(
            policy_df, key=[False] * i + [True], level=POLICIES[:i + 1])

    arch_df = _get_stats_df(rolling_df, ARCHITECTURES)
    out['architecture']['keys'] = []
    for arch in ARCHITECTURES:
        out['architecture']['keys'].append(arch)
        out['architecture'][arch] = _get_stats(
            arch_df, key=[True], level=[arch])

    impl_df = _get_stats_df(rolling_df, IMPLEMENTATIONS)
    out['implementation']['keys'] = []
    for impl in IMPLEMENTATIONS:
        out['implementation']['keys'].append(impl)
        out['implementation'][impl] = _get_stats(
            impl_df, key=[True], level=[impl])

    with open(utils.DATA_PATH, 'w') as f:
        json.dump(out, f, separators=(',', ':'))

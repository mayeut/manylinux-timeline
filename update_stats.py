import itertools
import json
import logging

from datetime import datetime, timezone
from io import StringIO
from string import Template

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


def _get_full_dataframe():
    df = pd.read_csv(
        utils.CACHE_NAME,
        names=utils.Row._fields,
        converters={'week': lambda x: pd.to_datetime(utils.from_week_str(x))}
    )
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
    return df


def _get_stat(stats, key, level):
    try:
        return stats.xs(key, level=level).agg('sum')
    except KeyError:
        return 0.0


def _get_graph(dataframe, kind):
    fig = dataframe.plot(kind=kind)
    fig.update_layout(xaxis_title=None, yaxis_title=None)
    fig.update_layout(yaxis_tickformat=',.1%')
    fig.update_layout(xaxis_tickangle=45)
    fig.update_layout(margin={'l': 0, 'r': 0, 't': 0, 'b': 80})
    fig.update_layout(hovermode='x unified')
    fig.update_traces(hovertemplate=None)
    fig.update_layout(legend_orientation='h', legend_title_text=None,
                      legend_xanchor='center', legend_x=0.5,
                      legend_yanchor='bottom', legend_y=1.0)
    with StringIO() as html:
        fig.write_html(html, config={'displayModeBar': False},
                       include_plotlyjs=False, include_mathjax=False,
                       full_html=False)
        return html.getvalue()


def update(start, end):
    pd.set_option('display.max_columns', None)
    df = _get_full_dataframe()
    date = pd.to_datetime(end)  # start at end
    start_date = pd.to_datetime(start)
    rows_highest_policy = []
    rows_lowest_policy = []
    rows_impl = []
    rows_arch = []
    index = []
    package_count = df[
        (df['week'] >= (start_date -utils.WINDOW_SIZE)) & (df['week'] < date)
    ][['package']].drop_duplicates().agg('count')['package']
    while date >= start_date:
        window_start = date - utils.WINDOW_SIZE
        df_window = \
            df[(df['week'] >= window_start) & (df['week'] < date)].sort_values(
                'week', ascending=False).drop_duplicates(['package'])
        index.append(date)
        date -= utils.WEEK_DELTA
        stats_policy = df_window[df_window['x86_64']].value_counts(
            subset=list(POLICIES), normalize=True)
        stats_arch = df_window.value_counts(subset=list(ARCHITECTURES),
                                            normalize=True)
        stats_impl = df_window.value_counts(subset=list(IMPLEMENTATIONS),
                                            normalize=True)
        len_ = len(POLICIES)
        rows_highest_policy.append(tuple(
            _get_stat(stats_policy,
                      tuple([True] + [False] * (len_ - i - 1)),
                      tuple(POLICIES[i:]))
            for i in range(len_)
        ))
        rows_lowest_policy.append(tuple(
            _get_stat(stats_policy,
                      tuple([False] * i + [True]),
                      tuple(POLICIES[:i + 1]))
            for i in range(len_)
        ))
        rows_arch.append(tuple(_get_stat(stats_arch, (True,), (arch,))
                               for arch in ARCHITECTURES))
        rows_impl.append(tuple(_get_stat(stats_impl, (True,), (impl,))
                               for impl in IMPLEMENTATIONS))

    out = {
        'last_update': datetime.now(timezone.utc).strftime(
            '%A, %d %B %Y, %H:%M:%S %Z'),
        'package_count': int(package_count),
        'index': list([d.date().isoformat() for d in index]),
        'lowest_policy': {},
        'highest_policy': {},
        'implementation': {},
        'architecture': {},
    }

    out['lowest_policy']['keys'] = [series.replace('ml', 'manylinux')
                                    for series in POLICIES]
    out['highest_policy']['keys'] = out['lowest_policy']['keys']
    for i, series in enumerate(POLICIES):
        series_name = series.replace('ml', 'manylinux')
        out['lowest_policy'][series_name] = [
            float(f'{row[i]:.4f}') for row in rows_lowest_policy]
        out['highest_policy'][series_name] = [
            float(f'{row[i]:.4f}') for row in rows_highest_policy]

    out['implementation']['keys'] = [series for series in IMPLEMENTATIONS]
    for i, series_name in enumerate(IMPLEMENTATIONS):
        out['implementation'][series_name] = [
            float(f'{row[i]:.4f}') for row in rows_impl]

    out['architecture']['keys'] = [series for series in ARCHITECTURES]
    for i, series_name in enumerate(ARCHITECTURES):
        out['architecture'][series_name] = [
            float(f'{row[i]:.4f}') for row in rows_arch]

    with open('data.json', 'w') as f:
        json.dump(out, f, separators=(',', ':'))

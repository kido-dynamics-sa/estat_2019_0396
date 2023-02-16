import dataclasses
import datetime
from enum import Enum
from typing import Dict, List

import pandas as pd

from .digest_generation import LONG_DT, Digest, digest_generation_iter


def series_to_events(times: pd.Series, cells: pd.Series) -> List[Dict]:
    return [{"time": time, "cell": cell} for time, cell in zip(times, cells)]


def digest_asdict(digest):
    def convert_value(obj):
        if isinstance(obj, Enum):
            return obj.value
        return obj

    return dict((k, convert_value(v)) for k, v in digest)


DIGEST_COLUMNS = [f.name for f in dataclasses.fields(Digest)]


def digest_to_dataframe(digests: List[Digest]) -> pd.DataFrame:
    if digests:
        return pd.DataFrame(
            [
                dataclasses.asdict(digest, dict_factory=digest_asdict)
                for digest in digests
            ]
        ).astype({"type": "string"})
    else:
        return pd.DataFrame(columns=DIGEST_COLUMNS)


def digest_to_dataframe_clipped(
    digests: List[Digest], min_time: datetime.datetime, max_time: datetime.datetime
) -> pd.DataFrame:
    clipped_digests = [
        dataclasses.asdict(digest, dict_factory=digest_asdict)
        for digest in digests
        if min_time <= digest.start_time <= max_time
    ]
    if clipped_digests:
        return pd.DataFrame(clipped_digests).astype({"type": "string"})
    else:
        return pd.DataFrame(columns=DIGEST_COLUMNS)


def digest_single_user(times: pd.Series, cells: pd.Series, **kwargs) -> pd.DataFrame:
    return digest_to_dataframe(digest_generation_iter(times, cells, **kwargs))


def clip_until_first_renewal(times: pd.Series, renewal_dt: int):
    """Return the location before the first renewal jump.

    If no renewal jumps present, return the location of the last point.
    """
    end_of_memory = (times.shift(-1) - times) > pd.Timedelta(renewal_dt, unit="s")
    if end_of_memory.any():
        return end_of_memory[end_of_memory].index[0]
    else:
        return times.index[-1]


def clip_from_last_renewal(times: pd.Series, renewal_dt: int):
    """Return the location after the last renewal jump.

    If no renewal jumps present, return the location of the first point.
    """
    start_of_memory = (times - times.shift(1)) > pd.Timedelta(renewal_dt, unit="s")
    if start_of_memory.any():
        return start_of_memory[start_of_memory].index[-1]
    else:
        return times.index[0]


def digest_single_user_clip(
    times: pd.Series,
    cells: pd.Series,
    min_time: datetime.datetime,
    max_time: datetime.datetime,
    **kwargs,
) -> pd.DataFrame:
    """Only consider digests that _start_ between min_time and max_time."""
    # try:
    renewal_dt = kwargs.get("long_dt", LONG_DT)
    warmup = times[times < min_time]
    last_warmup_renewal = (
        clip_from_last_renewal(warmup, renewal_dt)
        if len(warmup) > 0
        else times.index[0]
    )
    buffer = times[times > max_time]
    first_buffer_renewal = (
        clip_until_first_renewal(buffer, renewal_dt)
        if len(buffer) > 0
        else times.index[-1]
    )
    return digest_to_dataframe_clipped(
        digest_generation_iter(
            times.loc[last_warmup_renewal:first_buffer_renewal],
            cells.loc[last_warmup_renewal:first_buffer_renewal],
            **kwargs,
        ),
        min_time,
        max_time,
    )
    # except KeyError:
    #     return pd.DataFrame(columns=DIGEST_COLUMNS)


def digest_multi_user(
    df: pd.DataFrame,
    user_col: str = "user",
    time_col: str = "time",
    cell_col: str = "cell",
    user_props: List[str] = [],
    **kwargs,
) -> pd.DataFrame:
    digest_df = (
        df.sort_values(by=[user_col, time_col])
        .groupby([user_col] + user_props, group_keys=True)
        .apply(
            lambda x: digest_single_user(
                x.reset_index(drop=True)[time_col],
                x.reset_index(drop=True)[cell_col],
                **kwargs,
            ).rename_axis("digest_id")
        )
    )
    return digest_df if digest_df.empty else digest_df.reset_index()


def digest_multi_user_clip(
    df: pd.DataFrame,
    min_time: datetime.datetime,
    max_time: datetime.datetime,
    user_col: str = "user",
    time_col: str = "time",
    cell_col: str = "cell",
    user_props: List[str] = [],
    **kwargs,
) -> pd.DataFrame:
    digest_df = (
        df.sort_values(by=[user_col, time_col])
        .groupby([user_col] + user_props, group_keys=True)
        .apply(
            lambda x: digest_single_user_clip(
                x.reset_index(drop=True)[time_col],
                x.reset_index(drop=True)[cell_col],
                min_time,
                max_time,
                **kwargs,
            ).rename_axis("digest_id")
        )
    )
    return digest_df if digest_df.empty else digest_df.reset_index()

import datetime
from typing import Dict, List, Tuple

import pandas as pd

from .digest_pandas import digest_multi_user_clip


def generate_digests_observation_window(
    events,
    ow_start: datetime.datetime,
    ow_end: datetime.datetime,
    time_col: str = "time",
    user_col: str = "user",
    user_props: List[str] = [],
    **kwargs,
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, int]]]:

    # metadata about the events
    warmup_mask = events[time_col] < ow_start
    buffer_mask = events[time_col] > ow_end
    meta = {
        "warmup": {
            "duration": (ow_start - events[time_col].min()) / pd.Timedelta("1s"),
            "events": events[warmup_mask].shape[0],
            "users": events.loc[warmup_mask, user_col].nunique(),
        },
        "buffer": {
            "duration": (events[time_col].max() - ow_end) / pd.Timedelta("1s"),
            "events": events[buffer_mask].shape[0],
            "users": events.loc[buffer_mask, user_col].nunique(),
        },
        "observation": {
            "duration": (ow_end - ow_start) / pd.Timedelta("1s"),
            "events": events[~buffer_mask & ~warmup_mask].shape[0],
            "users": events.loc[~buffer_mask & ~warmup_mask, user_col].nunique(),
        },
    }
    digests = digest_multi_user_clip(
        events,
        user_props=user_props,
        min_time=ow_start,
        max_time=ow_end,
        time_col=time_col,
        user_col=user_col,
        **kwargs,
    )

    return digests, meta

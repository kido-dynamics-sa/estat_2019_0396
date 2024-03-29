import enum
from typing import Callable, List, Optional

import numpy as np
import pandas as pd

MAX_SPEED = 30 * 1000 / 3600  # 30 km/h


def footprint_distance(fp1: pd.Series, fp2: pd.Series) -> pd.Series:
    return pd.Series(np.ones(fp1.shape[0])) * np.inf


class TimePeriod(enum.Enum):
    daily = "D"
    weekly = "W"
    monthly = "M"


def get_permanence(
    footprints: pd.Series,
    times: pd.Series,
    max_speed: float = MAX_SPEED,
    distance_func: Callable = footprint_distance,
    semi_time_threshold: int = 8 * 60,
    max_dt: int = 12 * 60 * 60,
    time_grouping: Optional[TimePeriod] = None,
) -> pd.Series:

    if time_grouping:
        axis = [footprints, times.dt.to_period(time_grouping.value).dt.start_time]
        levels = [0, 1]
    else:
        axis = footprints
        levels = [0]

    times = times.set_axis(axis)
    footprints = footprints.set_axis(axis)
    dts = (times - times.shift(1)) / pd.Timedelta("1s")
    same_footprint = ((footprints == footprints.shift(1)) & (dts < max_dt)).values
    semi_times = np.minimum(0.5 * dts, semi_time_threshold) + np.minimum(
        0.5 * (times.shift(-1) - times) / pd.Timedelta("1s"), semi_time_threshold
    )
    low_speed = (
        distance_func(footprints.shift(1).values, footprints.shift(-1).values)
        / (dts.values + dts.shift(-1).values)
        < max_speed
    ) & ((dts.values + dts.shift(-1).values) < max_dt)

    d = dts.loc[same_footprint]
    s = semi_times.loc[(~same_footprint & low_speed).values]
    return (
        pd.concat(
            [
                d,
                s,
            ]
        )
        .groupby(level=levels)
        .sum()
        .rename("permanence_time")
    )


def permanence_multi_user(
    df: pd.DataFrame,
    user_col: str = "user",
    time_col: str = "time",
    footprint_col: str = "cell",
    user_props: List[str] = [],
    **kwargs,
) -> pd.DataFrame:
    permanence = (
        df.sort_values(by=[user_col, time_col])
        .groupby([user_col] + user_props, group_keys=True)
        .apply(
            lambda x: get_permanence(
                x.reset_index(drop=True)[footprint_col],
                x.reset_index(drop=True)[time_col],
                **kwargs,
            )
        )
    )
    return permanence if permanence.empty else permanence.reset_index()

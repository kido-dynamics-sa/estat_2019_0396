from typing import Callable

import numpy as np
import pandas as pd

MAX_SPEED = 30 * 1000 / 3600  # 30 km/h


def footprint_distance(fp1: pd.Series, fp2: pd.Series) -> pd.Series:
    return pd.Series(np.ones(fp1.shape[0])) * np.inf


def get_permanence(
    footprints: pd.Series,
    times: pd.Series,
    max_speed: float = MAX_SPEED,
    distance_func: Callable = footprint_distance,
) -> pd.Series:

    times = times.set_axis(footprints)
    footprints = footprints.set_axis(footprints)
    same_footprint = (footprints == footprints.shift(1)).values
    dts = (times - times.shift(1)) / pd.Timedelta("1s")
    semi_times = 0.5 * (dts + (times.shift(-1) - times) / pd.Timedelta("1s"))
    mean_speed = distance_func(
        footprints.shift(1).values, footprints.shift(-1).values
    ) / (dts.values + dts.shift(-1).values)

    print(mean_speed)
    print(dts)
    d = dts.loc[same_footprint]
    s = semi_times.loc[(~same_footprint & (mean_speed < max_speed)).values]
    print(d)
    print(s)
    return (
        pd.concat(
            [
                d,
                s,
            ]
        )
        .groupby(level=0)
        .sum()
    )

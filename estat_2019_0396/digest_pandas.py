import dataclasses
from enum import Enum
from typing import Dict, List

import pandas as pd

from .digest_generation import Digest, digest_generation


def series_to_events(times: pd.Series, cells: pd.Series) -> List[Dict]:
    return [{"time": time, "cell": cell} for time, cell in zip(times, cells)]


def digest_asdict(digest):
    def convert_value(obj):
        if isinstance(obj, Enum):
            return obj.value
        return obj

    return dict((k, convert_value(v)) for k, v in digest)


def digest_to_dataframe(digests: List[Digest]) -> pd.DataFrame:
    return pd.DataFrame(
        [dataclasses.asdict(digest, dict_factory=digest_asdict) for digest in digests]
    ).astype({"type": "string"})


def digest_single_user(times: pd.Series, cells: pd.Series) -> pd.DataFrame:
    return digest_to_dataframe(digest_generation(series_to_events(times, cells)))


def digest_multi_user(
    df: pd.DataFrame,
    user_col: str = "user",
    time_col: str = "time",
    cell_col: str = "cell",
) -> pd.DataFrame:
    return (
        df.sort_values(by=["user", "time"])
        .groupby(user_col)
        .apply(lambda x: digest_single_user(x[time_col], x[cell_col]))
        .reset_index()
        .rename(columns={"level_1": "digest_id"})
    )

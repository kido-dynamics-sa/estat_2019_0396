import dataclasses
from enum import Enum
from typing import Dict, List

import pandas as pd

from .digest_generation import Digest, digest_generation_iter


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
    return digest_to_dataframe(digest_generation_iter(times, cells))


def digest_multi_user(
    df: pd.DataFrame,
    user_col: str = "user",
    time_col: str = "time",
    cell_col: str = "cell",
    user_props: List[str] = [],
) -> pd.DataFrame:
    ngroups = 1 + len(user_props)
    return (
        df.sort_values(by=[user_col, time_col])
        .groupby([user_col] + user_props)
        .apply(
            lambda x: digest_single_user(
                x.reset_index(drop=True)[time_col], x.reset_index(drop=True)[cell_col]
            )
        )
        .reset_index()
        .rename(columns={f"level_{ngroups}": "digest_id"})
    )

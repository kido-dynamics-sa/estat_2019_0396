import dataclasses
from enum import Enum
from typing import List, Tuple

import pandas as pd

from .digest_generation import Digest


def series_to_events(times: pd.Series, cells: pd.Series) -> List[Tuple]:
    return list(zip(times, cells))


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

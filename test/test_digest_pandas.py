import datetime

import pandas as pd

from estat_2019_0396.digest_generation import Digest, DigestType
from estat_2019_0396.digest_pandas import digest_to_dataframe, series_to_events


def events_from_str(elist):
    fmt = "%Y-%m-%d %H:%M:%S"
    return [
        {"time": datetime.datetime.strptime(e[0], fmt), "cell": e[1]} for e in elist
    ]


def test_series_to_events():
    times = pd.date_range("2022-01-01 01:00:00", "2022-01-01 05:00:00", freq="1H")
    cells = pd.Series(["A"] * 5)
    events = series_to_events(times, cells)
    assert len(events) == 5
    assert events[0] == (datetime.datetime(2022, 1, 1, 1, 0, 0), "A")
    assert events[-1] == (datetime.datetime(2022, 1, 1, 5, 0, 0), "A")


def test_dataframe_single():
    digests = [
        Digest(
            start_time=datetime.datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime.datetime(2022, 1, 1, 5, 0, 0),
            cells=set(["A", "B", "C"]),
            num_cells=3,
            num_events=16,
            type=DigestType.ShortThreeCell,
        )
    ]
    df = digest_to_dataframe(digests)
    print(df.T)
    assert isinstance(df, pd.DataFrame)
    assert "start_time" in df.columns
    assert "end_time" in df.columns
    assert "cells" in df.columns
    assert "num_cells" in df.columns
    assert "num_events" in df.columns
    assert "type" in df.columns
    assert df.shape == (1, 6)


def test_dataframe_dtypes():
    digests = [
        Digest(
            start_time=datetime.datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime.datetime(2022, 1, 1, 5, 0, 0),
            cells=set(["A", "B", "C"]),
            num_cells=3,
            num_events=16,
            type=DigestType.ShortThreeCell,
        )
    ]
    df = digest_to_dataframe(digests)
    print(df.T)
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df["type"].dtype, pd.StringDtype)


def test_dataframe_many():
    digests = [
        Digest(
            start_time=datetime.datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime.datetime(2022, 1, 1, 5, 0, 0),
            cells=set(["A", "B", "C"]),
            num_cells=3,
            num_events=16,
            type=DigestType.ShortThreeCell,
        ),
        Digest(
            start_time=datetime.datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime.datetime(2022, 1, 5, 0, 0, 0),
            cells=set("A"),
            num_cells=1,
            num_events=150,
            type=DigestType.LongOneCell,
        ),
        Digest(
            start_time=datetime.datetime(2022, 1, 5, 8, 0, 0),
            end_time=datetime.datetime(2022, 1, 5, 8, 12, 0),
            cells=set(["A", "B"]),
            num_cells=2,
            num_events=4,
            type=DigestType.ShortTwoCell,
        ),
    ]
    df = digest_to_dataframe(digests)
    print(df)
    assert df.shape == (len(digests), 6)


#     digests = [
#     {
#         "start_time": "2021-08-15T10:00:00",
#         "cells": [
#             "A"
#         ],
#         "num_events": 1,
#         "num_cells": 1,
#         "type": "1-cell-repetition",
#         "end_time": "2021-08-15T10:00:00"
#     },
#     {
#         "start_time": "2021-08-18T10:00:00",
#         "cells": [
#             "A"
#         ],
#         "num_events": 1,
#         "num_cells": 1,
#         "type": "1-cell-repetition",
#         "end_time": "2021-08-18T10:00:00"
#     },
#     {
#         "start_time": "2021-09-15T10:00:00",
#         "cells": [
#             "A"
#         ],
#         "num_events": 2,
#         "num_cells": 1,
#         "type": "1-cell-repetition",
#         "end_time": "2021-09-15T10:00:01"
#     },
#     {
#         "start_time": "2022-01-01T10:00:00",
#         "cells": [
#             "A"
#         ],
#         "num_events": 2,
#         "num_cells": 1,
#         "type": "1-cell-repetition",
#         "end_time": "2022-01-01T12:00:00"
#     },
#     {
#         "start_time": "2022-01-01T12:01:00",
#         "cells": [
#             "B",
#             "A"
#         ],
#         "num_events": 6,
#         "num_cells": 2,
#         "type": "2-cell-flapping",
#         "end_time": "2022-01-01T12:01:10"
#     },
#     {
#         "start_time": "2022-01-01T12:01:10",
#         "cells": [
#             "B"
#         ],
#         "num_events": 6,
#         "num_cells": 1,
#         "type": "1-cell-repetition",
#         "end_time": "2022-01-01T18:00:00"
#     }
# ]

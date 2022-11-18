import datetime

import pandas as pd
import pytest

from estat_2019_0396.digest_generation import Digest, DigestType
from estat_2019_0396.digest_pandas import (
    digest_multi_user,
    digest_single_user,
    digest_to_dataframe,
    series_to_events,
)


@pytest.fixture()
def sample_digests():
    return [
        Digest(
            start_time=datetime.datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime.datetime(2022, 1, 1, 5, 0, 0),
            events_in_cell={"A": 10, "B": 4, "C": 3},
            start_cell="A",
            num_cells=3,
            num_events=16,
            type=DigestType.ShortThreeCell,
        )
    ]


@pytest.fixture()
def simple_events_df():
    return pd.DataFrame(
        {
            "time": pd.date_range(
                "2022-01-01 01:00:00", "2022-01-01 05:00:00", freq="1H"
            ),
            "cell": pd.Series(["A"] * 5),
        }
    )


@pytest.fixture()
def mixed_events_df():
    elist = [
        ["2021-08-15 10:00:00", "A"],
        ["2021-08-18 10:00:00", "A"],
        ["2021-09-15 10:00:00", "A"],
        ["2021-09-15 10:00:01", "A"],
        ["2022-01-01 10:00:00", "A"],
        ["2022-01-01 12:00:00", "A"],
        ["2022-01-01 12:01:00", "B"],
        ["2022-01-01 12:01:04", "A"],
        ["2022-01-01 12:01:05", "B"],
        ["2022-01-01 12:01:06", "B"],
        ["2022-01-01 12:01:07", "A"],
        ["2022-01-01 12:01:10", "B"],
        ["2022-01-01 14:00:00", "B"],
        ["2022-01-01 15:00:00", "B"],
        ["2022-01-01 16:00:00", "B"],
        ["2022-01-01 17:00:00", "B"],
        ["2022-01-01 18:00:00", "B"],
    ]
    return pd.DataFrame(
        {
            "time": pd.to_datetime([e[0] for e in elist]),
            "cell": pd.Series([e[1] for e in elist]),
        }
    )


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
    assert events[0]["time"] == datetime.datetime(2022, 1, 1, 1, 0, 0)
    assert events[0]["cell"] == "A"
    assert events[-1]["time"] == datetime.datetime(2022, 1, 1, 5, 0, 0)
    assert events[-1]["cell"] == "A"


def test_dataframe_single(sample_digests):
    df = digest_to_dataframe(sample_digests)
    print(df.T)
    assert isinstance(df, pd.DataFrame)
    assert "start_time" in df.columns
    assert "end_time" in df.columns
    assert "start_cell" in df.columns
    assert "end_cell" in df.columns
    assert "events_in_cell" in df.columns
    assert "num_cells" in df.columns
    assert "num_events" in df.columns
    assert "type" in df.columns
    assert df.shape == (1, 8)


def test_dataframe_dtypes(sample_digests):
    df = digest_to_dataframe(sample_digests)
    print(df.T)
    assert isinstance(df, pd.DataFrame)
    assert isinstance(df["type"].dtype, pd.StringDtype)


def test_dataframe_many():
    digests = [
        Digest(
            start_time=datetime.datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime.datetime(2022, 1, 1, 5, 0, 0),
            events_in_cell={"A": 10, "B": 5, "C": 1},
            start_cell="A",
            end_cell="B",
            num_cells=3,
            num_events=16,
            type=DigestType.ShortThreeCell,
        ),
        Digest(
            start_time=datetime.datetime(2022, 1, 1, 1, 0, 0),
            end_time=datetime.datetime(2022, 1, 5, 0, 0, 0),
            start_cell="A",
            end_cell="A",
            events_in_cell={"A": 150},
            num_cells=1,
            num_events=150,
            type=DigestType.LongOneCell,
        ),
        Digest(
            start_time=datetime.datetime(2022, 1, 5, 8, 0, 0),
            end_time=datetime.datetime(2022, 1, 5, 8, 12, 0),
            events_in_cell={"A": 2, "B": 2},
            num_cells=2,
            num_events=4,
            start_cell="A",
            end_cell="A",
            type=DigestType.ShortTwoCell,
        ),
    ]
    df = digest_to_dataframe(digests)
    print(df)
    assert df.shape == (len(digests), 8)


def test_digest_single():
    times = pd.date_range("2022-01-01 01:00:00", "2022-01-01 05:00:00", freq="1H")
    cells = pd.Series(["A"] * 5)
    df = digest_single_user(times, cells)
    print(df.T)
    assert isinstance(df, pd.DataFrame)
    assert "start_time" in df.columns
    assert "end_time" in df.columns
    assert "start_cell" in df.columns
    assert "end_cell" in df.columns
    assert "events_in_cell" in df.columns
    assert "num_cells" in df.columns
    assert "num_events" in df.columns
    assert "type" in df.columns
    assert df.shape == (1, 8)


def test_digest_multi_user_simple_sorted(simple_events_df):
    events_df = pd.concat(
        {"Agent1": simple_events_df},
        names=["user"],
    )

    df = digest_multi_user(events_df)
    print(df)
    assert isinstance(df, pd.DataFrame)
    assert "user" in df.columns
    assert "digest_id" in df.columns
    assert "start_time" in df.columns
    assert "end_time" in df.columns
    assert "start_cell" in df.columns
    assert "end_cell" in df.columns
    assert "events_in_cell" in df.columns
    assert "num_cells" in df.columns
    assert "num_events" in df.columns
    assert "type" in df.columns
    assert df.shape == (1, 10)


def test_digest_multi_user_many_sorted(simple_events_df, mixed_events_df):
    events_df = pd.concat(
        {
            "Agent1": simple_events_df,
            "Agent2": mixed_events_df,
        },
        names=["user"],
    )

    df = digest_multi_user(events_df)
    print(df)
    assert df["user"].nunique() == 2
    num_digest_per_user = df.groupby("user")["start_time"].count()
    assert num_digest_per_user["Agent1"] == 1
    assert num_digest_per_user["Agent2"] == 6


def test_digest_multi_user_indexing(simple_events_df, mixed_events_df):
    events_df = pd.concat(
        {
            "Agent1": simple_events_df,
            "Agent2": mixed_events_df,
        },
        names=["user"],
    )

    df = digest_multi_user(events_df)
    print(df)
    assert df[["user", "digest_id"]].drop_duplicates().shape[0] == df.shape[0]


def test_digest_multi_user_many_unsorted(simple_events_df, mixed_events_df):
    events_df = pd.concat(
        {
            "Agent1": simple_events_df,
            "Agent2": mixed_events_df,
        },
        names=["user"],
    ).sample(frac=1, random_state=47252)

    df = digest_multi_user(events_df)
    print(events_df)
    print(df)
    assert df["user"].nunique() == 2
    num_digest_per_user = df.groupby("user")["start_time"].count()
    assert num_digest_per_user["Agent1"] == 1
    assert num_digest_per_user["Agent2"] == 6

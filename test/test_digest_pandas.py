import datetime

import pandas as pd
import pytest

from estat_2019_0396.digest_generation import Digest, DigestType
from estat_2019_0396.digest_pandas import (
    clip_from_last_renewal,
    clip_until_first_renewal,
    digest_multi_user,
    digest_multi_user_clip,
    digest_single_user,
    digest_single_user_clip,
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
    times = pd.date_range("2022-01-02 01:00:00", "2022-01-02 05:00:00", freq="1H")
    cells = pd.Series(["A"] * len(times))
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


def test_digest_multi_user_params(simple_events_df):
    def max_hours(df):
        return ((df["end_time"] - df["start_time"]) / pd.Timedelta("1H")).max()

    events_df = simple_events_df
    events_df["user"] = "Agent1"

    assert max_hours(digest_multi_user(events_df)) == 4
    assert max_hours(digest_multi_user(events_df, cutoff=61 * 60)) == 2
    assert max_hours(digest_multi_user(events_df, cutoff=59 * 60)) == 1
    assert max_hours(digest_multi_user(events_df, long_dt=61 * 60)) == 4
    assert max_hours(digest_multi_user(events_df, long_dt=59 * 60)) == 0


@pytest.fixture()
def irregular_times():
    return pd.Series(
        pd.to_datetime(
            [
                "2022-01-01 08:00:00",
                "2022-01-01 09:00:00",
                "2022-01-01 20:00:00",
                "2022-01-01 21:00:00",
                "2022-01-01 22:00:00",
                "2022-01-02 10:00:00",
                "2022-01-02 10:00:00",  # g
                "2022-01-03 10:00:01",  # h
                "2022-01-03 10:00:02",
                "2022-01-03 20:00:00",
            ]
        ),
        index=list("abcdefghij"),
    )


def test_clip_until_first_renewal(irregular_times):
    print((irregular_times.shift(-1) - irregular_times).dt.seconds / (60 * 60))
    assert (
        clip_until_first_renewal(irregular_times, 8000 * 60 * 60)
        == irregular_times.index[-1]
    )
    assert clip_until_first_renewal(irregular_times, 0) == irregular_times.index[0]
    assert clip_until_first_renewal(irregular_times, 8 * 60 * 60) == "b"
    assert clip_until_first_renewal(irregular_times, 24 * 60 * 60) == "g"


def test_clip_from_last_renewal(irregular_times):
    print((irregular_times - irregular_times.shift(1)).dt.seconds / (60 * 60))
    assert (
        clip_from_last_renewal(irregular_times, 8000 * 60 * 60)
        == irregular_times.index[0]
    )
    assert clip_from_last_renewal(irregular_times, 0) == irregular_times.index[-1]
    assert clip_from_last_renewal(irregular_times, 8 * 60 * 60) == "j"
    assert clip_from_last_renewal(irregular_times, 24 * 60 * 60) == "h"


def test_digest_single_user_clip_donothing(irregular_times):
    cells = pd.Series(["A"] * len(irregular_times), index=irregular_times.index)
    df_noclip = digest_single_user(irregular_times, cells)
    df_clipped = digest_single_user_clip(
        irregular_times,
        cells,
        min_time=irregular_times.min(),
        max_time=irregular_times.max(),
    )
    cols = ["start_time", "end_time", "num_events"]
    print(df_noclip[cols])
    print(df_clipped[cols])
    assert len(df_noclip) == len(df_clipped)
    for col in cols:
        assert df_noclip[col].iloc[0] == df_clipped[col].iloc[0]
        assert df_noclip[col].iloc[-1] == df_clipped[col].iloc[-1]


def test_digest_single_user_clip_donothing2(simple_events_df):
    df_noclip = digest_single_user(simple_events_df["time"], simple_events_df["cell"])

    df_clipped = digest_single_user_clip(
        simple_events_df["time"],
        simple_events_df["cell"],
        min_time=simple_events_df["time"].min(),
        max_time=simple_events_df["time"].max(),
    )
    cols = ["start_time", "end_time", "num_events"]
    print(df_noclip[cols])
    print(df_clipped[cols])
    assert len(df_noclip) == len(df_clipped)
    for col in cols:
        assert df_noclip[col].iloc[0] == df_clipped[col].iloc[0]
        assert df_noclip[col].iloc[-1] == df_clipped[col].iloc[-1]


def test_digest_single_user_clip_diff(irregular_times):
    cells = pd.Series(["A"] * len(irregular_times), index=irregular_times.index)
    df_noclip = digest_single_user(irregular_times, cells)
    df_clipped = digest_single_user_clip(
        irregular_times,
        cells,
        min_time=datetime.datetime.strptime("2022-01-01 17:00:00", "%Y-%m-%d %H:%M:%S"),
        max_time=datetime.datetime.strptime("2022-01-02 23:59:59", "%Y-%m-%d %H:%M:%S"),
    )
    print(df_noclip, df_clipped)
    assert len(df_noclip) != len(df_clipped)


def test_digest_single_user_clip_same(irregular_times):
    cells = pd.Series(["A"] * len(irregular_times), index=irregular_times.index)
    df_clipped = digest_single_user_clip(
        irregular_times,
        cells,
        min_time=datetime.datetime.strptime("2022-01-01 17:00:00", "%Y-%m-%d %H:%M:%S"),
        max_time=datetime.datetime.strptime("2022-01-02 23:59:59", "%Y-%m-%d %H:%M:%S"),
    )
    df_nobuff = digest_single_user(irregular_times.loc["c":"g"], cells.loc["c":"g"])
    print(irregular_times.loc["c":"g"])
    cols = ["start_time", "end_time", "num_events"]
    print(df_nobuff[cols])
    print(df_clipped[cols])
    assert len(df_nobuff) == len(df_clipped)
    for col in cols:
        assert df_nobuff[col].iloc[0] == df_clipped[col].iloc[0]
        assert df_nobuff[col].iloc[-1] == df_clipped[col].iloc[-1]


def test_digest_single_user_clip_safe(irregular_times):
    """Test that clipping the warmup/buffer does not affect the results."""
    cells = pd.Series(["A"] * len(irregular_times), index=irregular_times.index)
    df_clipped = digest_single_user_clip(
        irregular_times,
        cells,
        min_time=datetime.datetime.strptime("2022-01-01 17:00:00", "%Y-%m-%d %H:%M:%S"),
        max_time=datetime.datetime.strptime("2022-01-02 23:59:59", "%Y-%m-%d %H:%M:%S"),
    )
    df_simple = digest_single_user(irregular_times, cells)
    df_manual_clip = df_simple[
        df_simple["start_time"].between("2022-01-01 17:00:00", "2022-01-02 23:59:59")
    ]

    cols = ["start_time", "end_time", "num_events"]
    print(df_manual_clip[cols])
    print(df_clipped[cols])
    assert len(df_simple) != len(df_clipped)
    assert len(df_manual_clip) == len(df_clipped)
    for col in cols:
        assert df_manual_clip[col].iloc[0] == df_clipped[col].iloc[0]
        assert df_manual_clip[col].iloc[-1] == df_clipped[col].iloc[-1]


def test_digest_multi_user_clip_donothing(simple_events_df, mixed_events_df):
    events_df = pd.concat(
        {
            "Agent1": simple_events_df,
            "Agent2": mixed_events_df,
        },
        names=["user"],
    )

    print(events_df)
    print(events_df["time"].min(), events_df["time"].max())
    df1 = digest_multi_user(events_df)
    df2 = digest_multi_user_clip(
        events_df,
        min_time=events_df["time"].min(),
        max_time=events_df["time"].max(),
    )
    cols = ["user", "start_time", "end_time", "num_events"]
    print(df1[cols])
    print(df2[cols])
    assert len(df1) == len(df2)
    assert hash(df1.to_csv()) == hash(df2.to_csv())


def test_digest_single_user_empty():
    assert digest_single_user(pd.Series(), pd.Series()).empty


def test_digest_mutli_user_empty():
    assert digest_multi_user(pd.DataFrame(columns=["user", "time", "cell"])).empty


def test_digest_multi_user_clip_empty(simple_events_df, mixed_events_df):
    events_df = pd.concat(
        {
            "Agent1": simple_events_df,
            "Agent2": mixed_events_df,
        },
        names=["user"],
    )

    print(events_df)
    print(events_df["time"].max(), events_df["time"].min())
    assert digest_multi_user_clip(
        events_df,
        min_time=events_df["time"].max() + pd.Timedelta("1d"),
        max_time=events_df["time"].min() - pd.Timedelta("1d"),
    ).empty

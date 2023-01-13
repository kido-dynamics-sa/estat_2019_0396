import numpy as np
import pandas as pd
import pytest

from estat_2019_0396.permanence import get_permanence


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


@pytest.fixture()
def infinite_distance():
    def footprint_distance(fp1: pd.Series, fp2: pd.Series) -> pd.Series:
        return pd.Series(np.ones(fp1.shape[0])) * np.inf

    return footprint_distance


@pytest.fixture()
def zero_distance():
    def footprint_distance(fp1: pd.Series, fp2: pd.Series) -> pd.Series:
        return pd.Series(np.zeros(fp1.shape[0]))

    return footprint_distance


def test_get_permanence_single():
    p = get_permanence(
        pd.Series(["A"]), pd.to_datetime(["2021-08-15 10:00:00"]).to_series()
    )
    print("RESULT", p)
    assert p.shape == (0,)


def test_get_permanence_static(simple_events_df):
    p = get_permanence(simple_events_df["cell"], simple_events_df["time"])
    print("RESULT", p)
    assert p.shape == (1,)
    assert p["A"] == (
        simple_events_df["time"].max() - simple_events_df["time"].min()
    ) / pd.Timedelta("1s")


def test_get_permanence_static_signature(simple_events_df, infinite_distance):
    p = get_permanence(
        simple_events_df["cell"],
        simple_events_df["time"],
        distance_func=infinite_distance,
    )
    print("RESULT", p)
    assert p.shape == (1,)
    assert p["A"] == (
        simple_events_df["time"].max() - simple_events_df["time"].min()
    ) / pd.Timedelta("1s")


def test_get_permanence_mixed(mixed_events_df, infinite_distance):
    p = get_permanence(
        mixed_events_df["cell"],
        mixed_events_df["time"],
        distance_func=infinite_distance,
    )
    print("RESULT", p)
    assert p.shape == (2,)
    assert p["A"] == 2 * 60 * 60
    assert (
        p["B"] == 1 + 6 * 60 * 60 - 60 - 10
    )  # second between 12:01:04 and 12:01:06 + seconds between  12:01:10 and 18:00:00


def test_get_permanence_triplet(zero_distance):
    elist = [
        ["2022-01-01 10:00:00", "A"],
        ["2022-01-01 12:00:00", "B"],
        ["2022-01-01 12:10:00", "A"],
    ]
    times = pd.to_datetime([e[0] for e in elist]).to_series()
    cells = pd.Series([e[1] for e in elist])
    p = get_permanence(
        cells, times, distance_func=zero_distance, semi_time_threshold=999999
    )
    print("RESULT", p)
    assert p.shape == (1,)
    assert p["B"] == 60 * 60 + 5 * 60  # half of 2h + half of 10 minutes


def test_get_permanence_threshold(zero_distance):
    elist = [
        ["2022-01-01 10:00:00", "A"],
        ["2022-01-01 12:00:00", "B"],
        ["2022-01-01 12:10:00", "A"],
    ]
    times = pd.to_datetime([e[0] for e in elist]).to_series()
    cells = pd.Series([e[1] for e in elist])
    p = get_permanence(
        cells, times, distance_func=zero_distance, semi_time_threshold=10 * 60
    )
    print("RESULT", p)
    assert p.shape == (1,)
    assert p["B"] == (10 + 5) * 60


def test_get_permanence_slow_speed():
    def unit_distance(fp1: pd.Series, fp2: pd.Series) -> pd.Series:
        return pd.Series(np.ones(fp1.shape[0]))

    max_speed = 1 / 3600  # 1 m/h
    elist = [
        ["2022-01-01 10:00:00", "A"],
        ["2022-01-01 10:10:00", "B"],
        ["2022-01-01 10:20:00", "C"],
        ["2022-01-01 13:00:00", "D"],
        ["2022-01-01 13:30:00", "E"],
        ["2022-01-01 14:30:00", "F"],
    ]
    times = pd.to_datetime([e[0] for e in elist]).to_series()
    cells = pd.Series([e[1] for e in elist])
    p = get_permanence(
        cells,
        times,
        distance_func=unit_distance,
        max_speed=max_speed,
        semi_time_threshold=9999,
    )
    print("RESULT", p)
    assert p.shape == (3,)
    assert p["C"] == 5 * 60 + (40 * 60 + 2 * 60 * 60) / 2
    assert p["D"] == (30 * 60 + 40 * 60 + 2 * 60 * 60) / 2
    assert p["E"] == 1.5 * 60 * 60 / 2


def test_get_permanence_maxdt(simple_events_df):
    p = get_permanence(simple_events_df["cell"], simple_events_df["time"], max_dt=5)
    print("RESULT", p)
    assert p.shape == (0,)

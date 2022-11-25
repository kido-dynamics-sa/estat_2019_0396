import dataclasses
import datetime
import json

import pytest

from estat_2019_0396.digest_generation import (
    Digest,
    DigestEncoder,
    DigestType,
    create_digest,
    digest_generation,
    digest_generation_dict,
    digest_generation_iter,
)


@pytest.fixture
def algo_default_params():
    return {
        "short_dt": 15,
        "long_dt": 8 * 60 * 60,  # 8 hours
        "cutoff": 24 * 60 * 60,  # 1 day
    }


def events_from_str(elist):
    fmt = "%Y-%m-%d %H:%M:%S"
    return [
        {"time": datetime.datetime.strptime(e[0], fmt), "cell": e[1]} for e in elist
    ]


def times_cells_from_str(elist):
    fmt = "%Y-%m-%d %H:%M:%S"
    return [datetime.datetime.strptime(e[0], fmt) for e in elist], [e[1] for e in elist]


def test_create_digest():
    now = datetime.datetime.now()
    digest = create_digest(now, "someID")
    assert isinstance(digest, Digest)
    assert digest.start_time == now


def test_json_parser():
    now = datetime.datetime.now()
    digest = create_digest(now, "someID")
    res = json.dumps(dataclasses.asdict(digest), cls=DigestEncoder)
    assert isinstance(res, str)
    assert len(res) > 0


def test_digest_single_event(algo_default_params):
    single_event = events_from_str([["2022-01-01 10:00:00", "A"]])
    output = digest_generation(single_event, **algo_default_params)
    assert len(output) == 1
    assert output[0].start_time == single_event[0]["time"]
    assert output[0].end_time == single_event[0]["time"]
    assert output[0].num_events == 1
    assert output[0].num_cells == 1
    assert output[0].cells == set("A")
    assert output[0].start_cell == output[0].end_cell == "A"


def test_digest_many_event_single_cell(algo_default_params):
    times = [
        "2022-01-01 10:00:00",
        "2022-01-01 11:00:00",
        "2022-01-01 11:00:05",
        "2022-01-01 11:00:08",
        "2022-01-01 12:00:00",
        "2022-01-01 15:00:01",
    ]
    many_event_single_cell = events_from_str([[time, "Acell"] for time in times])
    output = digest_generation(many_event_single_cell, **algo_default_params)
    assert len(output) == 1
    assert output[0].start_time == many_event_single_cell[0]["time"]
    assert output[0].end_time == many_event_single_cell[-1]["time"]
    assert output[0].num_events == len(many_event_single_cell)
    assert output[0].num_cells == 1
    assert output[0].cells == set(["Acell"])
    assert output[0].start_cell == output[0].end_cell == "Acell"


def test_digest_simple_2cell_flapping(algo_default_params):
    elist = [
        ["2022-01-01 10:00:00", "A"],
        ["2022-01-01 10:00:05", "B"],
    ]
    output = digest_generation(events_from_str(elist), **algo_default_params)
    assert len(output) == 1
    digest = output[0]
    print(digest)
    assert digest.start_time == datetime.datetime(2022, 1, 1, 10, 0, 0)
    assert digest.cells == set(["A", "B"])
    assert digest.num_events == len(elist)
    assert digest.num_cells == 2
    assert digest.type == DigestType.ShortTwoCell
    assert digest.end_time == datetime.datetime(2022, 1, 1, 10, 0, 5)
    assert digest.start_cell == "A"
    assert digest.end_cell == "B"


def test_digest_a_bit_of_everything(algo_default_params):
    elist = [
        ["2021-08-15 10:00:00", "A"],
        ["2021-08-18 10:00:00", "A"],
        ["2021-09-15 10:00:00", "A"],
        ["2021-09-15 10:00:01", "A"],
        ["2022-01-01 10:00:00", "A"],
        ["2022-01-01 12:00:00", "A"],
        ["2022-01-01 12:01:00", "B1"],
        ["2022-01-01 12:01:04", "A"],
        ["2022-01-01 12:01:05", "B1"],
        ["2022-01-01 12:01:06", "B1"],
        ["2022-01-01 12:01:07", "A"],
        ["2022-01-01 12:01:10", "B1"],
        ["2022-01-01 14:00:00", "B1"],
        ["2022-01-01 15:00:00", "B1"],
        ["2022-01-01 16:00:00", "B1"],
        ["2022-01-01 17:00:00", "B1"],
        ["2022-01-01 18:00:00", "B1"],
    ]
    output = digest_generation(events_from_str(elist), **algo_default_params)
    assert len(output) == 6
    assert output[0] == Digest(
        start_time=datetime.datetime(2021, 8, 15, 10, 0, 0),
        start_cell="A",
        events_in_cell={"A": 1},
        num_events=1,
        num_cells=1,
        type=DigestType.ShortOneCell,
        end_time=datetime.datetime(2021, 8, 15, 10, 0, 0),
        end_cell="A",
    )
    assert output[-1] == Digest(
        start_time=datetime.datetime(2022, 1, 1, 12, 1, 10),
        start_cell="B1",
        events_in_cell={"B1": 6},
        num_events=6,
        num_cells=1,
        type=DigestType.LongOneCell,
        end_time=datetime.datetime(2022, 1, 1, 18, 0, 0),
        end_cell="B1",
    )


def test_digest_long_3cell_flapping(algo_default_params):
    elist = [
        ["2022-01-01 10:00:00", "A"],
        ["2022-01-01 10:00:05", "B"],
        ["2022-01-01 10:00:10", "A"],
        ["2022-01-01 10:00:15", "A"],
        ["2022-01-01 10:00:20", "A"],
        ["2022-01-01 10:00:25", "B"],
        ["2022-01-01 10:00:30", "C"],
        ["2022-01-01 10:00:35", "A"],
        ["2022-01-01 10:00:40", "C"],
        ["2022-01-01 10:00:45", "C"],
        ["2022-01-01 10:00:50", "B"],
        ["2022-01-01 10:00:55", "C"],
    ]
    output = digest_generation(events_from_str(elist), **algo_default_params)
    assert len(output) == 1
    digest = output[0]
    print(digest)
    assert digest.start_time == datetime.datetime(2022, 1, 1, 10, 0, 0)
    assert digest.cells == set(["A", "B", "C"])
    assert digest.num_events == len(elist)
    assert digest.num_cells == 3
    assert digest.type == DigestType.ShortThreeCell
    assert digest.end_time == datetime.datetime(2022, 1, 1, 10, 0, 55)
    assert digest.start_cell == "A"
    assert digest.end_cell == "C"


def test_digest_back2back(algo_default_params):
    elist = [
        ["2022-01-01 12:01:00", "A"],
        ["2022-01-01 12:01:02", "B"],
        ["2022-01-01 12:01:04", "A"],
        ["2022-01-01 12:01:05", "B"],
        ["2022-01-01 12:01:06", "B"],
        ["2022-01-01 12:01:07", "A"],
        ["2022-01-01 14:00:00", "A"],
        ["2022-01-01 15:00:00", "A"],
    ]
    output = digest_generation(events_from_str(elist), **algo_default_params)
    assert len(output) == 2
    assert output[0].end_time == output[1].start_time
    assert sum(o.num_events for o in output) == len(elist) + 1
    return


def test_digest_iter():
    elist = [
        ["2022-01-01 12:01:00", "A"],
        ["2022-01-01 12:01:02", "B"],
        ["2022-01-01 12:01:04", "A"],
        ["2022-01-01 12:01:05", "B"],
        ["2022-01-01 12:01:06", "B"],
        ["2022-01-01 12:01:07", "A"],
        ["2022-01-01 14:00:00", "A"],
        ["2022-01-01 15:00:00", "A"],
    ]
    output_dict = digest_generation_dict(events_from_str(elist))
    output_iter = digest_generation_iter(*times_cells_from_str(elist))
    assert output_dict == output_iter


def test_digest_total_consistency():
    elist = [
        ["2021-08-15 10:00:00", "A"],
        ["2021-08-18 10:00:00", "A"],
        ["2021-09-15 10:00:00", "A"],
        ["2021-09-15 10:00:01", "A"],
        ["2022-01-01 10:00:00", "A"],
        ["2022-01-01 12:00:00", "A"],
        ["2022-01-01 12:01:00", "B1"],
        ["2022-01-01 12:01:04", "A"],
        ["2022-01-01 12:01:05", "B1"],
        ["2022-01-01 12:01:06", "B1"],
        ["2022-01-01 12:01:07", "A"],
        ["2022-01-01 12:01:10", "B1"],
        ["2022-01-01 14:00:00", "B1"],
        ["2022-01-01 15:00:00", "B1"],
        ["2022-01-01 16:00:00", "B1"],
        ["2022-01-01 17:00:00", "B1"],
        ["2022-01-01 18:00:00", "B1"],
    ]
    digests = digest_generation(events_from_str(elist))

    for digest in digests:
        assert digest.num_cells == len(digest.events_in_cell.keys())
        assert digest.num_events == sum(digest.events_in_cell.values())


def test_digest_short_dt():
    elist = [
        ["2022-01-01 10:00:00", "A"],
        ["2022-01-01 10:00:05", "B"],
        ["2022-01-01 10:00:10", "C"],
        ["2022-01-01 10:00:15", "A"],
        ["2022-01-01 10:00:20", "B"],
        ["2022-01-01 10:00:25", "C"],
        ["2022-01-01 10:00:30", "A"],
        ["2022-01-01 10:00:35", "B"],
        ["2022-01-01 10:00:40", "C"],
        ["2022-01-01 10:00:45", "A"],
        ["2022-01-01 10:00:50", "B"],
        ["2022-01-01 10:00:55", "C"],
    ]
    events = events_from_str(elist)
    assert len(digest_generation(events, short_dt=4)) == len(elist)
    assert len(digest_generation(events, short_dt=5)) == len(elist)
    assert len(digest_generation(events, short_dt=6)) == 1


def test_digest_long_dt():
    times = [
        "2022-01-01 10:00:00",
        "2022-01-01 11:00:00",
        "2022-01-01 12:00:00",
        "2022-01-01 13:00:00",
        "2022-01-01 14:00:00",
        "2022-01-01 15:00:00",
    ]
    many_event_single_cell = events_from_str([[time, "Acell"] for time in times])

    assert len(digest_generation(many_event_single_cell, long_dt=30 * 60)) == len(times)
    assert len(digest_generation(many_event_single_cell, long_dt=60 * 60)) == len(times)
    assert len(digest_generation(many_event_single_cell, long_dt=60 * 60 + 1)) == 1
    assert len(digest_generation(many_event_single_cell, long_dt=2 * 60 * 60)) == 1


def test_digest_cutoff():
    times = [
        "2022-01-01 10:00:00",
        "2022-01-01 11:00:00",
        "2022-01-01 12:00:00",
        "2022-01-01 13:00:00",
        "2022-01-01 14:00:00",
        "2022-01-01 15:00:00",
    ]
    many_event_single_cell = events_from_str([[time, "Acell"] for time in times])

    assert len(digest_generation(many_event_single_cell, cutoff=24 * 60 * 60)) == 1
    assert (
        len(digest_generation(many_event_single_cell, cutoff=2 * 60 * 60))
        == len(times) // 3
    )
    assert (
        len(digest_generation(many_event_single_cell, cutoff=1 * 60 * 60))
        == len(times) // 2
    )
    assert len(digest_generation(many_event_single_cell, cutoff=45 * 60)) == len(times)


def test_digest_empty():
    assert digest_generation([]) == []
    assert digest_generation_iter([], []) == []

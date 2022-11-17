import dataclasses
import datetime
import json

from estat_2019_0396.digest_generation import (
    Digest,
    DigestEncoder,
    DigestType,
    create_digest,
    digest_generation,
    digest_generation_dict,
    digest_generation_iter,
)


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


def test_digest_single_event():
    single_event = events_from_str([["2022-01-01 10:00:00", "A"]])
    output = digest_generation(single_event)
    assert len(output) == 1
    assert output[0].start_time == single_event[0]["time"]
    assert output[0].end_time == single_event[0]["time"]
    assert output[0].num_events == 1
    assert output[0].num_cells == 1
    assert output[0].cells == set("A")


def test_digest_many_event_single_cell():
    times = [
        "2022-01-01 10:00:00",
        "2022-01-01 11:00:00",
        "2022-01-01 11:00:05",
        "2022-01-01 11:00:08",
        "2022-01-01 12:00:00",
        "2022-01-01 15:00:01",
    ]
    many_event_single_cell = events_from_str([[time, "Acell"] for time in times])
    output = digest_generation(many_event_single_cell)
    assert len(output) == 1
    assert output[0].start_time == many_event_single_cell[0]["time"]
    assert output[0].end_time == many_event_single_cell[-1]["time"]
    assert output[0].num_events == len(many_event_single_cell)
    assert output[0].num_cells == 1
    assert output[0].cells == set(["Acell"])


def test_digest_a_bit_of_everything():
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
    output = digest_generation(events_from_str(elist))
    assert len(output) == 6
    assert output[0] == Digest(
        start_time=datetime.datetime(2021, 8, 15, 10, 0, 0),
        cells=set("A"),
        num_events=1,
        num_cells=1,
        type=DigestType.ShortOneCell,
        end_time=datetime.datetime(2021, 8, 15, 10, 0, 0),
    )
    assert output[-1] == Digest(
        start_time=datetime.datetime(2022, 1, 1, 12, 1, 10),
        cells=set(["B1"]),
        num_events=6,
        num_cells=1,
        type=DigestType.LongOneCell,
        end_time=datetime.datetime(2022, 1, 1, 18, 0, 0),
    )


def test_digest_long_3cell_flapping():
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
        ["2022-01-01 10:00:55", "A"],
    ]
    output = digest_generation(events_from_str(elist))
    assert len(output) == 1
    assert output[0] == Digest(
        start_time=datetime.datetime(2022, 1, 1, 10, 0, 0),
        cells=set(["A", "B", "C"]),
        num_events=len(elist),
        num_cells=3,
        type=DigestType.ShortThreeCell,
        end_time=datetime.datetime(2022, 1, 1, 10, 0, 55),
    )


def test_digest_back2back():
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
    output = digest_generation(events_from_str(elist))
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

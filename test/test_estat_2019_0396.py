import dataclasses
import datetime
import json

import pytest

from estat_2019_0396.estat_2019_0396 import (
    Digest,
    DigestEncoder,
    DigestType,
    create_digest,
    digest_generation,
)


def events_from_str(elist):
    fmt = "%Y-%m-%d %H:%M:%S"
    return [
        {"time": datetime.datetime.strptime(e[0], fmt), "cell": e[1]} for e in elist
    ]


@pytest.fixture
def single_event():
    return events_from_str([["2022-01-01 10:00:00", "A"]])


@pytest.fixture
def many_event_single_cell():
    times = [
        "2022-01-01 10:00:00",
        "2022-01-01 11:00:00",
        "2022-01-01 11:00:05",
        "2022-01-01 11:00:08",
        "2022-01-01 12:00:00",
        "2022-01-01 15:00:01",
    ]
    return events_from_str([[time, "A"] for time in times])


@pytest.fixture
def a_bit_of_everything():
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
    return events_from_str(elist)


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


def test_digest_single_event(single_event):
    output = digest_generation(single_event)
    assert len(output) == 1
    assert output[0].start_time == single_event[0]["time"]
    assert output[0].end_time == single_event[0]["time"]
    assert output[0].num_events == 1
    assert output[0].num_cells == 1
    assert output[0].cells == set("A")


def test_digest_many_event_single_cell(many_event_single_cell):
    output = digest_generation(many_event_single_cell)
    assert len(output) == 1
    assert output[0].start_time == many_event_single_cell[0]["time"]
    assert output[0].end_time == many_event_single_cell[-1]["time"]
    assert output[0].num_events == len(many_event_single_cell)
    assert output[0].num_cells == 1
    assert output[0].cells == set("A")


def test_digest_a_bit_of_everything(a_bit_of_everything):
    output = digest_generation(a_bit_of_everything)
    assert output[0] == Digest(
        start_time=datetime.datetime(2021, 8, 15, 10, 0, 0),
        cells=set("A"),
        num_events=1,
        num_cells=1,
        type=DigestType.ShortOneCell,
        end_time=datetime.datetime(2021, 8, 15, 10, 0, 0),
    )
    assert output[-1] == Digest(
        start_time=datetime.datetime(2022, 1, 1, 14, 0, 0),
        cells=set("B"),
        num_events=5,
        num_cells=1,
        type=DigestType.LongOneCell,
        end_time=datetime.datetime(2022, 1, 1, 18, 0, 0),
    )

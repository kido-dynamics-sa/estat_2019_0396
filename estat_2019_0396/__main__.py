import dataclasses
import datetime
import json

from estat_2019_0396.digest_generation import DigestEncoder, digest_generation

if __name__ == "__main__":
    fmt = "%Y-%m-%d %H:%M:%S"
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
    events = [
        {"time": datetime.datetime.strptime(e[0], fmt), "cell": e[1]} for e in elist
    ]

    print(
        json.dumps(
            [dataclasses.asdict(d) for d in digest_generation(events)],
            cls=DigestEncoder,
            indent=4,
        )
    )

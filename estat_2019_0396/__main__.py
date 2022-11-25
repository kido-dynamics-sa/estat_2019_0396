import datetime
import enum
import json
from pathlib import Path
from typing import Optional

import pandas as pd
import typer

from estat_2019_0396 import digest_multi_user, generate_digests_observation_window


class Compression(enum.Enum):
    ZIP = "zip"
    GZIP = "gzip"


class Format(enum.Enum):
    csv = "csv"
    parquet = "parquet"


DEFAULT_FORMAT = Format.csv

input_file = typer.Argument(
    ...,
    exists=True,
    readable=True,
)

output_file = typer.Option(
    None,
    writable=True,
)


def read_dataset(path, format):
    if format == Format.csv:
        return pd.read_csv(path, parse_dates=["time"])
    elif format == Format.parquet:
        return pd.read_parquet(path)
    else:
        raise NotImplementedError(f"Unknown format: {format}")


def write_dataset(df, path, format, compression):
    if format == Format.csv:
        return df.to_csv(
            path, compression=compression.value if compression else None, index=False
        )
    elif format == Format.parquet:
        return df.to_parquet(
            path, compression=compression.value if compression else None, index=False
        )
    else:
        raise NotImplementedError(f"Unknown format: {format}")


def main(
    input_file: Path = input_file,
    output: Path = output_file,
    compression: Optional[Compression] = None,
    input_format: Format = DEFAULT_FORMAT,
    output_format: Format = DEFAULT_FORMAT,
):
    df = read_dataset(input_file, input_format)
    if "user_type" in df:
        user_props = ["user_type"]
    else:
        user_props = []
    print(
        write_dataset(
            digest_multi_user(df, user_props=user_props),
            output,
            output_format,
            compression,
        )
    )


def analysis(
    ow_start: datetime.datetime,
    ow_end: datetime.datetime,
    # input: Path = input_file,
    input_file: str = input_file,
    output: str = output_file,
    compression: Optional[Compression] = None,
    input_format: Format = DEFAULT_FORMAT,
    output_format: Format = DEFAULT_FORMAT,
    meta: bool = False,
):
    df = read_dataset(input_file, input_format)
    digests, metadata = generate_digests_observation_window(
        df, ow_start, ow_end, user_props=["user_type"]
    )
    print(
        write_dataset(
            digests,
            output,
            output_format,
            compression,
        )
    )
    if meta:
        print(json.dumps(metadata))


# def parametric_study(
#     input: Path = input_file,
#     output: Optional[Path] = output_file,
#     compression: Optional[Compression] = None,
# ):
#     df = pd.read_csv(input, parse_dates=["time"])
#     if "user_type" in df:
#         user_props = ["user_type"]
#     else:
#         user_props = []

#     short_dts = range(5, 61, 5)
#     # short_dts = range(5, 35, 10)
#     results = (
#         pd.concat(
#             {
#                 short_dt: digest_multi_user(
#                     df, user_props=user_props, short_dt=short_dt
#                 )
#                 .groupby(["user"] + user_props)["digest_id"]
#                 .count()
#                 for short_dt in short_dts
#             },
#             names=["short_dt"],
#         )
#         .rename("num_digests")
#         .reset_index(level="short_dt")
#     )
#     print(
#         results.to_csv(
#             output, compression=compression.value if compression else None, index=False
#         )
#     )


if __name__ == "__main__":
    # typer.run(main)
    # typer.run(parametric_study)
    typer.run(analysis)

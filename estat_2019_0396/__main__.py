import enum
from pathlib import Path
from typing import Optional

import pandas as pd
import typer

from estat_2019_0396 import digest_multi_user


class Compression(enum.Enum):
    ZIP = "zip"
    GZIP = "gzip"


def main(
    input: Path = typer.Argument(
        ...,
        exists=True,
        readable=True,
    ),
    output: Optional[Path] = typer.Option(
        None,
        writable=True,
    ),
    compression: Optional[Compression] = None,
):
    df = pd.read_csv(input, parse_dates=["time"])
    if "user_type" in df:
        user_props = ["user_type"]
    else:
        user_props = []
    print(
        digest_multi_user(df, user_props=user_props).to_csv(
            output, compression=compression.value if compression else None, index=False
        )
    )


if __name__ == "__main__":
    typer.run(main)

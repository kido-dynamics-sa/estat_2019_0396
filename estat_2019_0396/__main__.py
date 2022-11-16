import sys

import pandas as pd

from estat_2019_0396 import digest_multi_user

if __name__ == "__main__":
    file_in = sys.argv[1]

    df = pd.read_csv(file_in, parse_dates=["time"])
    print(digest_multi_user(df))

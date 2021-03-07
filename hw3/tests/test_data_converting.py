import pytest
import pandas as pd
from pathlib import Path
from pandas.testing import assert_frame_equal

from hw3.data_converting.data_converting_into_parquet import get_df_from_csv

content = """
id,hotel_cluster
0,99 1
1,99 1
2,99 1
"""


def test_get_parquet_from_csv(tmpdir):
    f1 = tmpdir.mkdir("mydir").join("myfile")
    f1.write(content)
    dict_for_compare_df = {"id": [0, 1, 2] ,"hotel_cluster": ["99 1", "99 1", "99 1"]}
    df = get_df_from_csv(f1)
    assert_frame_equal(
        pd.DataFrame(dict_for_compare_df),
        df,
        check_dtype=False,
    )


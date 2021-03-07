from pathlib import Path

import click
import pandas as pd

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--path_to_file", prompt="Path to csv file", help="Path to csv file")
def start_get_parquet_from_csv(path_to_file) -> None:
    """
    Run function get_parquet_from_csv with 'path_to_file' parameter, got as command line parameter.
    :param path_to_file:
    :return: None
    """
    df = get_df_from_csv(path_to_file)
    df.to_parquet(f"{Path(path_to_file).stem}.parquet")


def get_df_from_csv(path_to_file: str) -> pd.DataFrame:
    """
    Get path to csv-file, create pandas dataframe and create parquet-file
    :param path_to_file:
    :return: None
    """
    path_object = Path(path_to_file)
    return pd.read_csv(path_object, sep=",", encoding="utf8")


if __name__ == "__main__":
    start_get_parquet_from_csv()

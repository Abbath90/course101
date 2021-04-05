import pytest
from pyspark.sql import SparkSession

from hw6.pyspark_tasks.tasks.tasks import read_csv_and_create_df


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("test").getOrCreate()


@pytest.fixture(scope="session")
def tempfile_fd(tmpdir_factory, spark):
    df = read_csv_and_create_df(
        "./hw6/pyspark_tasks/tests/test_stuff/train_10k.csv", spark
    )
    return df

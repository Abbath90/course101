import click
import pyspark.sql

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--path_to_file", prompt="Path to csv file", help="Path to csv file")
@click.option(
    "--task",
    default="all",
    prompt="Number of task",
    help="""
task 1 - Find top 3 most popular hotels between couples. \n
task 2 - Find the most popular country where hotels are booked and searched from the same country. \n
task 3 - Find top 3 hotels where people with children are interested but not booked in the end. \n
task all - all tasks in a row (default)""",
)
@click.option(
    "--mode",
    default="show",
    prompt="Print of file mode",
    help="""
show - show result in stdout
file - write result in a file""",
)
def start_pyspark_tasks(path_to_file: str, task: str, mode: str) -> None:
    results = []
    if task in ("1", "2", "3", "all") and mode in ("show", "file"):
        spark = pyspark.sql.SparkSession.builder.appName("PySpark").getOrCreate()
        df = read_csv_and_create_df(path_to_file, spark)
        if task == "1":
            results.append(get_top_between_couples(df))
        elif task == "2":
            results.append(get_most_popular_country(df))
        elif task == "3":
            results.append(get_top_with_children_without_booking(df))
        elif task == "all":
            results.append(get_top_between_couples(df))
            results.append(get_most_popular_country(df))
            results.append(get_top_with_children_without_booking(df))
        for task_number, result in enumerate(results):
            if mode == "show":
                result.show()
            elif mode == "file":
                result.coalesce(1).write.csv(f"results/task_0{task_number}")
    else:
        print("incorrect input")


def read_csv_and_create_df(
    path_to_file: str, spark: pyspark.sql.SparkSession
) -> pyspark.sql.DataFrame:
    """
    Read csv file and create Spark dataframe.
    :param path_to_file: Path to csv file
    :param spark: Created spark session
    :return: pyspark Dataframe
    """
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(path_to_file)
    )
    df = df[
        [
            "hotel_continent",
            "hotel_country",
            "hotel_market",
            "is_booking",
            "srch_adults_cnt",
            "srch_children_cnt",
            "user_location_country",
        ]
    ]

    return df


def get_top_between_couples(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Get top 3 most popular hotels between couples.
    :param df: Pyspark Dataframe
    :return: Pyspark Dataframe
    """
    top_between_couples = (
        df[(df["is_booking"] == 1) & (df["srch_adults_cnt"] == 2)]
        .groupby(["hotel_continent", "hotel_country", "hotel_market"])
        .count()
        .orderBy("count", ascending=False)
        .limit(3)
    )

    return top_between_couples


def get_most_popular_country(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Get the most popular country where hotels are booked and searched from the same country.
    :param df: Pyspark Dataframe
    :return: Pyspark Dataframe
    """
    the_most_popular_country = (
        df[(df["hotel_country"] == df["user_location_country"])]
        .groupby(["hotel_country"])
        .count()
        .orderBy("count", ascending=False)
        .limit(1)
    )

    return the_most_popular_country


def get_top_with_children_without_booking(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Get top 3 hotels where people with children are interested but not booked in the end.
    :param df: Pyspark Dataframe
    :return: Pyspark Dataframe
    """
    top_with_children_without_booking = (
        df[(df["is_booking"] == 0) & (df["srch_children_cnt"] > 0)]
        .groupby(["hotel_continent", "hotel_country", "hotel_market"])
        .count()
        .orderBy("count", ascending=False)
        .limit(3)
    )

    return top_with_children_without_booking


if __name__ == "__main__":
    start_pyspark_tasks()

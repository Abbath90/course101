from chispa import assert_df_equality
from pyspark.sql.types import Row

from hw6.pyspark_tasks.tasks.tasks import (
    get_most_popular_country,
    get_top_between_couples,
    get_top_with_children_without_booking,
)


def test_get_top_between_couples(tempfile_fd):
    df = get_top_between_couples(tempfile_fd)

    assert df.collect() == [
        Row(hotel_continent=2, hotel_country=50, hotel_market=675, count=15),
        Row(hotel_continent=2, hotel_country=50, hotel_market=628, count=12),
        Row(hotel_continent=2, hotel_country=50, hotel_market=743, count=10),
    ]


def test_get_most_popular_country(tempfile_fd):
    df = get_most_popular_country(tempfile_fd)

    assert df.collect() == [Row(hotel_country=46, count=1)]


def test_get_top_with_children_without_booking(tempfile_fd):
    df = get_top_with_children_without_booking(tempfile_fd)

    assert df.collect() == [
        Row(hotel_continent=4, hotel_country=8, hotel_market=110, count=205),
        Row(hotel_continent=2, hotel_country=50, hotel_market=368, count=161),
        Row(hotel_continent=2, hotel_country=50, hotel_market=628, count=117),
    ]

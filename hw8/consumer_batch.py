from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StringType, StructField, StructType

topic_ip = "172.18.0.2:6667"
topic_name = "hotels"


def main():
    spark = SparkSession.builder.appName("Hotels").getOrCreate()

    schema = StructType(
        [
            StructField("date_time", StringType(), True),
            StructField("site_name", StringType(), True),
            StructField("posa_container", StringType(), True),
            StructField("user_location_country", StringType(), True),
            StructField("user_location_region", StringType(), True),
            StructField("user_location_city", StringType(), True),
            StructField("orig_destination_distance", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("is_mobile", StringType(), True),
            StructField("is_package", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("srch_ci", StringType(), True),
            StructField("srch_co", StringType(), True),
            StructField("srch_adults_cnt", StringType(), True),
            StructField("srch_children_cnt", StringType(), True),
            StructField("srch_rm_cnt", StringType(), True),
            StructField("srch_destination_id", StringType(), True),
            StructField("srch_destination_type_id", StringType(), True),
            StructField("is_booking", StringType(), True),
            StructField("cnt", StringType(), True),
            StructField("hotel_continent", StringType(), True),
            StructField("hotel_country", StringType(), True),
            StructField("hotel_market", StringType(), True),
            StructField("hotel_cluster", StringType(), True),
        ]
    )

    lines = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", topic_ip)
        .option("subscribe", topic_name)
        .load()
    )

    df = lines.select(
        functions.from_json(functions.col("value").cast("string"), schema).alias(
            "parse_value"
        )
    ).select(
        "parse_value.date_time",
        "parse_value.site_name",
        "parse_value.posa_container",
        "parse_value.user_location_country",
        "parse_value.user_location_region",
        "parse_value.user_location_city",
        "parse_value.orig_destination_distance",
        "parse_value.user_id",
        "parse_value.is_mobile",
        "parse_value.is_package",
        "parse_value.channel",
        "parse_value.srch_ci",
        "parse_value.srch_co",
        "parse_value.srch_adults_cnt",
        "parse_value.srch_children_cnt",
        "parse_value.srch_rm_cnt",
        "parse_value.srch_destination_id",
        "parse_value.srch_destination_type_id",
        "parse_value.is_booking",
        "parse_value.cnt",
        "parse_value.hotel_continent",
        "parse_value.hotel_country",
        "parse_value.hotel_market",
        "parse_value.hotel_cluster",
    )

    df.write.format("json").option(
        "checkpointLocation", "/home/hotel_data_batch/checkpoint"
    ).option("path", "/home/hotel_data_batch/data").save()


if __name__ == "__main__":
    main()

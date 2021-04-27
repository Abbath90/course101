import json
import random
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import click
from kafka import KafkaProducer

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

topic_ip = "172.18.0.2:6667"
topic_name = "hotels"
producer = KafkaProducer(bootstrap_servers=[topic_ip])


def get_random_date(start: str, end: str) -> datetime:
    date_format = "%d-%m-%Y %H:%M:%S"
    start_time = time.mktime(time.strptime(start, date_format))
    end_time = time.mktime(time.strptime(end, date_format))
    random_time = start_time + random.random() * (end_time - start_time)
    dt = datetime.fromtimestamp(time.mktime(time.localtime(random_time)))
    return dt


def generate_structure() -> str:
    start_date = "01-07-2013 00:00:00"
    end_date = "31-12-2015 23:59:59"
    random_date = get_random_date(start_date, end_date)
    random_date_arrival = random_date.date() + timedelta(days=random.randrange(180))
    random_date_departure = random_date_arrival + timedelta(days=random.randrange(60))
    output_struct = OrderedDict(
        [
            ("date_time", random_date),
            ("site_name", random.randrange(1, 37)),
            ("posa_continent", random.randrange(4)),
            ("user_location_country", random.randrange(216)),
            ("user_location_region", random.randrange(1000)),
            ("user_location_city", random.randrange(56500)),
            ("orig_destination_distance", random.randrange(10000)),
            ("user_id", random.randrange(1100000)),
            ("is_mobile", random.randrange(1)),
            ("is_package", random.randrange(1)),
            ("channel", random.randrange(10)),
            ("srch_ci", random_date_arrival),
            ("srch_co", random_date_departure),
            ("srch_adults_cnt", random.randrange(6)),
            ("srch_children_cnt", random.randrange(5)),
            ("srch_rm_cnt", random.randrange(4)),
            ("srch_destination_id", random.randrange(50000)),
            ("srch_destination_type_id", random.randrange(10)),
            ("is_booking", random.randrange(1)),
            ("cnt", 1),
            ("hotel_continent", random.randrange(7)),
            ("hotel_country", random.randrange(213)),
            ("hotel_market", random.randrange(2118)),
            ("hotel_cluster", random.randrange(100)),
        ]
    )
    return json.dumps(output_struct, default=str)


def message_producer():
    producer.send(topic_name, generate_structure().encode("utf-8"))
    producer.flush()


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--threads", prompt="Number of threads", help="Number of threads")
@click.option("--events", prompt="Number of events", help="Number of events")
def main(threads, events) -> None:
    with ThreadPoolExecutor(threads) as pool:
        sends = 0
        while sends < events:
            sends += 1
            pool.submit(message_producer)


if __name__ == "__main__":
    main()

import json
from datetime import datetime

from hw8.producer import generate_structure, get_random_date


def test_get_random_date():
    start = "01-07-2013 00:00:00"
    end = "01-07-2013 00:00:00"

    result = get_random_date(start, end)

    assert result == datetime.strptime("01-07-2013 00:00:00", "%d-%m-%Y %H:%M:%S")


def test_generate_structure(mocker):
    mocker.patch(
        "hw8.producer.get_random_date",
        return_value=datetime.strptime("01-07-2013 00:00:00", "%d-%m-%Y %H:%M:%S"),
    )
    output = generate_structure()
    assert json.loads(output)["date_time"] == "2013-07-01 00:00:00"

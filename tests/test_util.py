from datetime import datetime, timezone

import pytest
import pytz

from asphalt.tasks.util import (
    datetime_to_utc_timestamp, datetime_ceil, as_timezone, create_reference, convert_to_datetime)


@pytest.mark.parametrize('input, expected', [
    (None, None),
    (datetime(2014, 3, 12, 5, 40, 13, 254012), 1394599213.254012)
], ids=['none', 'datetime'])
def test_datetime_to_utc_timestamp(input, expected, timezone):
    if input:
        input = timezone.localize(input)

    assert datetime_to_utc_timestamp(input) == expected


@pytest.mark.parametrize('input, expected', [
    (datetime(2009, 4, 7, 2, 10, 16, 4000), datetime(2009, 4, 7, 2, 10, 17)),
    (datetime(2009, 4, 7, 2, 10, 16), datetime(2009, 4, 7, 2, 10, 16))
], ids=['milliseconds', 'exact'])
def test_datetime_ceil(input, expected):
    assert datetime_ceil(input) == expected


@pytest.mark.parametrize('input, expected', [
    ('Europe/Helsinki', pytz.timezone('Europe/Helsinki')),
    (pytz.timezone('Europe/Helsinki'), pytz.timezone('Europe/Helsinki'))
], ids=['text', 'tzinfo'])
def test_as_timezone(input, expected):
    assert as_timezone(input) == expected


@pytest.mark.parametrize('input, expected', [
    (None, None),
    (datetime(2016, 7, 24, 18, 36, 51, 134296), datetime(2016, 7, 24, 18, 36, 51, 134296)),
    (datetime(2016, 7, 24, 16, 36, 51, 134296, timezone.utc),
     datetime(2016, 7, 24, 18, 36, 51, 134296))
], ids=['none', 'naive', 'aware'])
def test_convert_to_datetime(input, expected, timezone):
    if expected:
        expected = timezone.localize(expected)

    assert convert_to_datetime(input, timezone) == expected


def test_create_reference():
    assert create_reference(create_reference) == 'asphalt.tasks.util:create_reference'


def test_create_reference_nested_function():
    def nested():
        pass

    exc = pytest.raises(ValueError, create_reference, nested)
    assert str(exc.value) == 'cannot create a reproducible reference to a nested function'

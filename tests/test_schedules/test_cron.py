from datetime import datetime
from typing import Any
from typing import Dict

import pytest
from asphalt.serialization.api import Serializer, CustomizableSerializer
from pytz.tzinfo import DstTzInfo

from asphalt.tasks.schedules.cron import CronSchedule
from asphalt.tasks.util import convert_to_datetime


@pytest.fixture
def schedule(timezone: DstTzInfo):
    return CronSchedule(task_id='taskname', id='testschedule',
                        start_time=datetime(2016, 7, 20, 16, 40),
                        end_time=datetime(2016, 12, 25, 6, 16), hour='*/2', minute=5,
                        timezone=timezone, args=[1, 6], kwargs={'argument': 'value'})


def test_bad_misfire_grace_time(timezone: DstTzInfo):
    exc = pytest.raises(ValueError, CronSchedule, task_id='taskname',
                        timezone=timezone, misfire_grace_time=0)
    assert str(exc.value) == 'misfire_grace_time must be a positive integer or None'


def test_bad_start_end_times(timezone: DstTzInfo):
    exc = pytest.raises(ValueError, CronSchedule, task_id='taskname', day='*',
                        start_time=datetime(2016, 3, 4, 12, 5, 3),
                        end_time=datetime(2016, 3, 4, 12, 5, 2), timezone=timezone)
    assert str(exc.value) == 'end_time cannot be earlier than start_time'


def test_serialize_deserialize(serializer: Serializer, schedule: CronSchedule):
    if isinstance(serializer, CustomizableSerializer):
        serializer.register_custom_type(CronSchedule)

    payload = serializer.serialize(schedule)
    deserialized = serializer.deserialize(payload)

    assert deserialized.start_time == schedule.start_time
    assert deserialized.end_time == schedule.end_time
    assert deserialized.fields == schedule.fields
    assert deserialized.timezone == schedule.timezone
    assert deserialized.args == schedule.args
    assert deserialized.kwargs == schedule.kwargs
    assert deserialized.misfire_grace_time == schedule.misfire_grace_time


def test_setstate_unhandled_version(schedule: CronSchedule):
    exc = pytest.raises(ValueError, schedule.__setstate__, {'version': 2})
    assert str(exc.value) == ('cannot deserialize CronSchedule definition newer than '
                              'version 1 (version 2 received)')


@pytest.mark.parametrize('previous_time, now, expected', [
    (None, datetime(2016, 7, 18), datetime(2016, 7, 20, 18, 5)),
    (datetime(2016, 7, 20, 18, 5), datetime(2016, 7, 20, 18, 5), datetime(2016, 7, 20, 20, 5)),
    (None, datetime(2016, 7, 20, 20, 43), datetime(2016, 7, 20, 22, 5))
], ids=['before_start', 'previous_runtime', 'after_start'])
def test_get_next_run_time(schedule: CronSchedule, previous_time, now, expected,
                           timezone: DstTzInfo):
    previous_time = convert_to_datetime(previous_time, timezone)
    now = convert_to_datetime(now, timezone)
    expected = convert_to_datetime(expected, timezone)
    assert schedule.get_next_run_time(now, previous_time) == expected


def test_month_rollover(timezone: DstTzInfo):
    """Test that if the maximum value in a field is reached, the previous field is incremented."""
    schedule = CronSchedule(task_id='task', timezone=timezone, day=30)
    now = timezone.localize(datetime(2016, 2, 1))
    expected = timezone.localize(datetime(2016, 3, 30))
    assert schedule.get_next_run_time(now) == expected


def test_weekday_increment_rollover(timezone: DstTzInfo):
    """
    Test that if a field's value exceeds the maximum value during a calculation, that field along
    with all the less significant ones are reset to their minimum values.

    """
    schedule = CronSchedule(task_id='task', timezone=timezone, day='8', day_of_week='fri')
    now = timezone.localize(datetime(2016, 3, 1))
    expected = timezone.localize(datetime(2016, 4, 8))
    assert schedule.get_next_run_time(now) == expected


@pytest.mark.parametrize('month, expression, expected_day', [
    (7, '5th sun', 31),
    (2, 'last mon', 29),
    (9, '1st wed', 7)
], ids=['july', 'february', 'september'])
def test_weekday_position(month: int, expression: str, expected_day: int, timezone: DstTzInfo):
    schedule = CronSchedule(task_id='task', timezone=timezone, day=expression)
    now = timezone.localize(datetime(2016, month, 1))
    expected = timezone.localize(datetime(2016, month, expected_day))
    assert schedule.get_next_run_time(now) == expected


def test_range_step(timezone: DstTzInfo):
    """Test that a range expression with a step value produces the correct values."""
    schedule = CronSchedule(task_id='task', timezone=timezone, day='5-24/3')
    previous_run_time = timezone.localize(datetime(2016, 2, 23))
    now = timezone.localize(datetime(2016, 3, 31))
    expected = [timezone.localize(datetime(2016, 3, day)) for day in (5, 8, 11, 14, 17, 20, 23)]
    run_times = list(schedule.get_run_times(now, previous_run_time))
    assert run_times == expected


@pytest.mark.parametrize('month, expected_day', [
    (1, 31),
    (2, 29),
    (4, 30)
], ids=['january', 'february', 'april'])
def test_last_day_of_month(month: int, expected_day: int, timezone: DstTzInfo):
    schedule = CronSchedule(task_id='task', timezone=timezone, day='last')
    now = timezone.localize(datetime(2016, month, 1))
    expected = timezone.localize(datetime(2016, month, expected_day))
    assert schedule.get_next_run_time(now) == expected


def test_start_time(schedule: CronSchedule, timezone: DstTzInfo):
    """Test that start_time is respected."""
    now = timezone.localize(datetime(2016, 1, 15))
    expected = timezone.localize(datetime(2016, 7, 20, 18, 5))
    assert schedule.get_next_run_time(now) == expected


def test_end_time(schedule: CronSchedule, timezone: DstTzInfo):
    """Test that end_time is respected."""
    now = timezone.localize(datetime(2020, 12, 31))
    assert schedule.get_next_run_time(now) is None


def test_dst_forward(timezone: DstTzInfo):
    schedule = CronSchedule(task_id='task', timezone=timezone, minute='*/5')
    previous_time = timezone.localize(datetime(2016, 3, 27, 1, 55))
    now = timezone.localize(datetime(2016, 3, 27, 1, 59))
    expected = timezone.localize(datetime(2016, 3, 27, 3))
    assert schedule.get_next_run_time(now, previous_time) == expected


def test_dst_backward(timezone: DstTzInfo):
    schedule = CronSchedule(task_id='task', timezone=timezone, minute='*/5')
    previous_time = timezone.localize(datetime(2016, 10, 30, 2, 55), is_dst=True)
    now = timezone.localize(datetime(2016, 10, 30, 2, 59), is_dst=True)
    expected = timezone.localize(datetime(2016, 10, 30, 2), is_dst=False)
    assert schedule.get_next_run_time(now, previous_time) == expected


@pytest.mark.parametrize('kwargs, result', [
    ({'hour': '*'}, "hour='*'"),
    ({'hour': '*/2'}, "hour='*/2'"),
    ({'hour': '5-8'}, "hour='5-8'"),
    ({'hour': '5-15/3'}, "hour='5-15/3'"),
    ({'day_of_week': 'mon,tue'}, "day_of_week='mon,tue'"),
    ({'day_of_week': 'mon-fri'}, "day_of_week='mon-fri'"),
    ({'day': '1st tue'}, "day='1st tue'"),
], ids=['all', 'all_step', 'range', 'range_step', 'weekday_list', 'weekday_range', 'posweekday'])
def test_repr(kwargs, result, timezone):
    schedule = CronSchedule(task_id='taskname', id='testschedule', timezone=timezone, **kwargs)
    assert repr(schedule) == "<CronSchedule (id='testschedule', task_id='taskname', %s)>" % result


@pytest.mark.parametrize('kwargs, message', [
    ({'year': '*/0'}, 'increment must be higher than 0'),
    ({'year': '2016-2015'}, 'the minimum value in a range must not be higher than the maximum'),
    ({'day_of_week': 'bleh'}, 'invalid weekday name "bleh"'),
    ({'day_of_week': 'mon-blah'}, 'invalid weekday name "blah"'),
    ({'day': '1st bleh'}, 'invalid weekday name "bleh"'),
    ({'day': 'bleh'}, 'unrecognized expression "bleh" for field "day"'),
], ids=['increment', 'range', 'weekdayname1', 'weekdayname2', 'weekdayposname', 'nomatch'])
def test_invalid_expression_values(kwargs, message: str, timezone: DstTzInfo):
    exc = pytest.raises(ValueError, CronSchedule, task_id='task', timezone=timezone, **kwargs)
    assert str(exc.value) == message


@pytest.mark.parametrize('kwargs', [
    {'day': 'last'},
    {'day': '4th fri'},
])
def test_fields_equality(kwargs: Dict[str, Any], timezone: DstTzInfo):
    """Test the equality of fields and their string forms for extra test coverage."""
    schedules = [CronSchedule(task_id='task', timezone=timezone, **kwargs) for _ in range(2)]
    assert schedules[0].fields == schedules[1].fields
    for field1, field2 in zip(schedules[0].fields, schedules[1].fields):
        assert str(field1) == str(field2)

from datetime import datetime, date

import pytest
from asphalt.serialization.api import Serializer, CustomizableSerializer
from pytz.tzinfo import DstTzInfo

from asphalt.tasks.schedules.calendarinterval import CalendarIntervalSchedule


@pytest.fixture
def schedule(timezone: DstTzInfo):
    return CalendarIntervalSchedule(
        task_id='taskname', id='testschedule', years=1, months=5, weeks=6, days=8,
        start_date=date(2016, 3, 5), end_date=date(2020, 12, 25), hour=3, second=8,
        timezone=timezone, args=[1, 6], kwargs={'argument': 'value'})


def test_bad_interval(timezone):
    exc = pytest.raises(ValueError, CalendarIntervalSchedule, task_id='taskname',
                        timezone=timezone)
    assert str(exc.value) == 'the interval must be at least 1 day long'


def test_bad_misfire_grace_time(timezone):
    exc = pytest.raises(ValueError, CalendarIntervalSchedule, task_id='taskname',
                        timezone=timezone, misfire_grace_time=0)
    assert str(exc.value) == 'misfire_grace_time must be a positive integer or None'


def test_bad_start_end_dates(timezone):
    exc = pytest.raises(ValueError, CalendarIntervalSchedule, task_id='taskname', days=1,
                        start_date=date(2016, 3, 4), end_date=date(2016, 3, 3), timezone=timezone)
    assert str(exc.value) == 'end_date cannot be earlier than start_date'


def test_serialize_deserialize(serializer: Serializer, schedule: CalendarIntervalSchedule):
    if isinstance(serializer, CustomizableSerializer):
        serializer.register_custom_type(CalendarIntervalSchedule)

    payload = serializer.serialize(schedule)
    deserialized = serializer.deserialize(payload)

    assert deserialized.years == schedule.years
    assert deserialized.months == schedule.months
    assert deserialized.weeks == schedule.weeks
    assert deserialized.days == schedule.days
    assert deserialized.time == schedule.time
    assert deserialized.start_date == schedule.start_date
    assert deserialized.end_date == schedule.end_date
    assert deserialized.timezone == schedule.timezone
    assert deserialized.args == schedule.args
    assert deserialized.kwargs == schedule.kwargs
    assert deserialized.misfire_grace_time == schedule.misfire_grace_time


def test_setstate_unhandled_version(schedule: CalendarIntervalSchedule):
    exc = pytest.raises(ValueError, schedule.__setstate__, {'version': 2})
    assert str(exc.value) == ('cannot deserialize CalendarIntervalSchedule definition newer than '
                              'version 1 (version 2 received)')


def test_get_next_run_times(schedule: CalendarIntervalSchedule, timezone: DstTzInfo):
    now = timezone.localize(datetime(2021, 7, 31, 5, 17))
    previous_run_time = timezone.localize(datetime(2016, 3, 5, 3, 0, 8))
    run_times = list(schedule.get_run_times(now, previous_run_time))
    assert run_times == [
        timezone.localize(datetime(2017, 9, 24, 3, 0, 8)),
        timezone.localize(datetime(2019, 4, 15, 3, 0, 8)),
        timezone.localize(datetime(2020, 11, 4, 3, 0, 8))
    ]


def test_start_date(schedule: CalendarIntervalSchedule, timezone: DstTzInfo):
    """Test that start_date is respected."""
    now = timezone.localize(datetime(2016, 1, 15))
    expected = timezone.localize(datetime(2016, 3, 5, 3, 0, 8))
    assert schedule.get_next_run_time(now) == expected


def test_end_date(schedule: CalendarIntervalSchedule, timezone: DstTzInfo):
    """Test that end_date is respected."""
    now = timezone.localize(datetime(2020, 12, 31))
    assert schedule.get_next_run_time(now) is None


def test_missing_time(timezone: DstTzInfo):
    """
    Test that if the designated time does not exist on a day due to a forward DST shift, the day is
    skipped entirely.

    """
    schedule = CalendarIntervalSchedule(task_id='task', timezone=timezone, days=1, hour=2,
                                        minute=30, start_date=date(2016, 3, 27))
    now = timezone.localize(datetime(2016, 3, 27))
    expected = timezone.localize(datetime(2016, 3, 28, 2, 30))
    assert schedule.get_next_run_time(now) == expected


def test_repeated_time(timezone: DstTzInfo):
    """
    Test that if the designated time is repeated during a day due to a backward DST shift, the task
    is executed twice that day.

    """
    schedule = CalendarIntervalSchedule(task_id='task', timezone=timezone, days=2, hour=2,
                                        minute=30, start_date=date(2016, 10, 30))

    # The first returned datetime should be the on still in DST
    now = timezone.localize(datetime(2016, 10, 30))
    expected = timezone.localize(datetime(2016, 10, 30, 2, 30), is_dst=True)
    assert schedule.get_next_run_time(now) == expected

    # The next one should then be the one w/o DST
    now = timezone.localize(datetime(2016, 10, 30, 2, 40), is_dst=True)
    expected = timezone.localize(datetime(2016, 10, 30, 2, 30), is_dst=False)
    assert schedule.get_next_run_time(now) == expected

    # But if both times have passed, move on to the next interval
    now = timezone.localize(datetime(2016, 10, 30, 2, 40), is_dst=False)
    expected = timezone.localize(datetime(2016, 11, 1, 2, 30))
    assert schedule.get_next_run_time(now) == expected


def test_nonexistent_days(timezone: DstTzInfo):
    """Test that invalid dates are skipped."""
    schedule = CalendarIntervalSchedule(task_id='task', timezone=timezone, months=1,
                                        start_date=date(2016, 3, 31))

    now = timezone.localize(datetime(2016, 4, 30))
    expected = timezone.localize(datetime(2016, 5, 31))
    assert schedule.get_next_run_time(now) == expected


def test_repr(schedule):
    assert repr(schedule) == ("<CalendarIntervalSchedule (id='testschedule', task_id='taskname', "
                              "years=1, months=5, weeks=6, days=8, time=03:00:08)>")

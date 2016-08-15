from datetime import datetime

import pytest
from asphalt.serialization.api import Serializer, CustomizableSerializer
from pytz.tzinfo import DstTzInfo

from asphalt.tasks.schedules.interval import IntervalSchedule
from asphalt.tasks.util import convert_to_timezone


@pytest.fixture
def start_time(timezone):
    return timezone.localize(datetime(2016, 7, 20, 16, 40))


@pytest.fixture
def end_time(timezone):
    return timezone.localize(datetime(2016, 12, 25, 6, 16))


@pytest.fixture
def schedule(start_time: datetime, end_time: datetime, timezone: DstTzInfo):
    return IntervalSchedule(task_id='taskname', id='testschedule', start_time=start_time,
                            end_time=end_time, minutes=2, seconds=5, timezone=timezone,
                            args=[1, 6], kwargs={'argument': 'value'})


def test_bad_misfire_grace_time(timezone):
    exc = pytest.raises(ValueError, IntervalSchedule, task_id='taskname',
                        timezone=timezone, seconds=1, misfire_grace_time=0)
    assert str(exc.value) == 'misfire_grace_time must be a positive integer or None'


def test_bad_interval(timezone):
    exc = pytest.raises(ValueError, IntervalSchedule, task_id='taskname', id='testschedule',
                        timezone=timezone)
    assert str(exc.value) == 'interval must be at least 1 second long'


def test_bad_start_end_times(timezone):
    exc = pytest.raises(ValueError, IntervalSchedule, task_id='taskname', days=1,
                        start_time=datetime(2016, 3, 4, 12, 5, 3),
                        end_time=datetime(2016, 3, 4, 12, 5, 2), timezone=timezone)
    assert str(exc.value) == 'end_time cannot be earlier than start_time'


def test_serialize_deserialize(serializer: Serializer, schedule: IntervalSchedule):
    if isinstance(serializer, CustomizableSerializer):
        serializer.register_custom_type(IntervalSchedule)

    payload = serializer.serialize(schedule)
    deserialized = serializer.deserialize(payload)

    assert deserialized.start_time == schedule.start_time
    assert deserialized.end_time == schedule.end_time
    assert deserialized.interval == schedule.interval
    assert deserialized.timezone == schedule.timezone
    assert deserialized.args == schedule.args
    assert deserialized.kwargs == schedule.kwargs
    assert deserialized.misfire_grace_time == schedule.misfire_grace_time


def test_setstate_unhandled_version(schedule: IntervalSchedule):
    exc = pytest.raises(ValueError, schedule.__setstate__, {'version': 2})
    assert str(exc.value) == ('cannot deserialize IntervalSchedule definition newer than '
                              'version 1 (version 2 received)')


@pytest.mark.parametrize('previous_time, now, expected', [
    (None, datetime(2016, 7, 18), datetime(2016, 7, 20, 16, 40)),
    (datetime(2016, 7, 20, 16, 40), datetime(2016, 7, 20, 16, 40, 1),
     datetime(2016, 7, 20, 16, 42, 5)),
    (None, datetime(2016, 7, 20, 16, 43), datetime(2016, 7, 20, 16, 43))
], ids=['before_start', 'previous_runtime', 'after_start'])
def test_get_next_run_time(schedule: IntervalSchedule, previous_time, now, expected,
                           timezone: DstTzInfo):
    previous_time = convert_to_timezone(previous_time, timezone)
    now = convert_to_timezone(now, timezone)
    expected = convert_to_timezone(expected, timezone)
    assert schedule.get_next_run_time(now, previous_time) == expected


def test_get_next_run_times(schedule: IntervalSchedule, timezone: DstTzInfo):
    now = timezone.localize(datetime(2016, 7, 31, 5, 17))
    previous_run_time = timezone.localize(datetime(2016, 7, 31, 5, 9, 55))
    expected = [
        timezone.localize(datetime(2016, 7, 31, 5, 12, 0)),
        timezone.localize(datetime(2016, 7, 31, 5, 14, 5)),
        timezone.localize(datetime(2016, 7, 31, 5, 16, 10))
    ]
    run_times = list(schedule.get_run_times(now, previous_run_time))
    assert run_times == expected


def test_resume_schedule(now: datetime, timezone: DstTzInfo):
    """Test that when resuming the schedule, the current time is returned."""
    schedule = IntervalSchedule(task_id='task', timezone=timezone, seconds=1)
    assert schedule.get_next_run_time(now) == now


def test_start_time(schedule: IntervalSchedule, timezone: DstTzInfo, start_time: datetime):
    """Test that start_time is respected."""
    now = timezone.localize(datetime(2016, 1, 15))
    assert schedule.get_next_run_time(now) == start_time


def test_end_time(schedule: IntervalSchedule, timezone: DstTzInfo):
    """Test that end_time is respected."""
    now = timezone.localize(datetime(2020, 12, 31))
    assert schedule.get_next_run_time(now) is None


def test_dst_forward(schedule: IntervalSchedule, timezone: DstTzInfo):
    previous_time = timezone.localize(datetime(2016, 3, 27, 1, 58))
    now = timezone.localize(datetime(2016, 3, 27, 1, 59))
    expected = timezone.localize(datetime(2016, 3, 27, 3, 0, 5))
    assert schedule.get_next_run_time(now, previous_time) == expected


def test_dst_backward(schedule: IntervalSchedule, timezone: DstTzInfo):
    previous_time = timezone.localize(datetime(2016, 10, 30, 2, 58), is_dst=True)
    now = timezone.localize(datetime(2016, 10, 30, 2, 59), is_dst=True)
    expected = timezone.localize(datetime(2016, 10, 30, 2, 0, 5), is_dst=False)
    assert schedule.get_next_run_time(now, previous_time) == expected


def test_repr(schedule):
    assert repr(schedule) == ("<IntervalSchedule (id='testschedule', task_id='taskname', "
                              "interval=datetime.timedelta(0, 125))>")

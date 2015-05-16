from datetime import datetime

import pytest
from asphalt.serialization.api import Serializer, CustomizableSerializer

from asphalt.tasks.schedules.date import DateSchedule
from pytz.tzinfo import DstTzInfo


@pytest.fixture
def schedule(now: datetime, timezone):
    return DateSchedule(task_id='taskname', id='testschedule', run_time=now, timezone=timezone,
                        args=[1, 6], kwargs={'argument': 'value'})


def test_bad_misfire_grace_time(timezone: DstTzInfo, now: datetime):
    exc = pytest.raises(ValueError, DateSchedule, task_id='taskname',
                        timezone=timezone, run_time=now, misfire_grace_time=0)
    assert str(exc.value) == 'misfire_grace_time must be a positive integer or None'


def test_serialize_deserialize(serializer: Serializer, schedule: DateSchedule):
    if isinstance(serializer, CustomizableSerializer):
        serializer.register_custom_type(DateSchedule)

    payload = serializer.serialize(schedule)
    deserialized = serializer.deserialize(payload)

    assert deserialized.run_time == schedule.run_time
    assert deserialized.timezone == schedule.timezone
    assert deserialized.args == schedule.args
    assert deserialized.kwargs == schedule.kwargs
    assert deserialized.misfire_grace_time == schedule.misfire_grace_time


def test_setstate_unhandled_version(schedule: DateSchedule):
    exc = pytest.raises(ValueError, schedule.__setstate__, {'version': 2})
    assert str(exc.value) == ('cannot deserialize DateSchedule definition newer than version 1 '
                              '(version 2 received)')


def test_get_next_run_time(schedule: DateSchedule, now: datetime):
    assert schedule.get_next_run_time(now, None) == now


def test_get_next_run_time_again(schedule: DateSchedule, now: datetime):
    assert schedule.get_next_run_time(now, now) is None


def test_repr(schedule: DateSchedule, now: datetime):
    assert repr(schedule) == ("<DateSchedule (id='testschedule', task_id='taskname', "
                              "run_time=%r)>" % now)

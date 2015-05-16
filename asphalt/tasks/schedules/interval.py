from datetime import timedelta, datetime
from typing import Optional, Dict, Any

from typeguard import check_argument_types

from asphalt.tasks.schedules.base import BaseSchedule
from asphalt.tasks.util import convert_to_datetime, datetime_to_utc_timestamp


class IntervalSchedule(BaseSchedule):
    """
    Runs the task on specified intervals.

    If no previous run time is specified when requesting a new run time (like when starting for the
    first time or resuming after being paused), either the current time or the start time is
    returned, whichever comes later. Otherwise, the interval is added to the previous run time and
    the result is returned.

    :param weeks: number of weeks to wait
    :param days: number of days to wait
    :param hours: number of hours to wait
    :param minutes: number of minutes to wait
    :param seconds: number of seconds to wait
    :param start_time: earliest possible datetime to run the task on
    :param end_time: latest possible datetime to run the task on
    """

    __slots__ = 'start_time', 'end_time', 'interval'

    def __init__(self, *, weeks: int = 0, days: int = 0, hours: int = 0, minutes: int = 0,
                 seconds: int = 0, start_time: datetime = None, end_time: datetime = None,
                 **kwargs):
        assert check_argument_types()
        super().__init__(**kwargs)
        self.start_time = convert_to_datetime(start_time, self.timezone)
        self.end_time = convert_to_datetime(end_time, self.timezone)
        self.interval = timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes,
                                  seconds=seconds)

        if self.interval.total_seconds() < 1:
            raise ValueError('interval must be at least 1 second long')
        if start_time and end_time and start_time > end_time:
            raise ValueError('end_time cannot be earlier than start_time')

    def get_next_run_time(self, now: datetime,
                          previous_run_time: datetime = None) -> Optional[datetime]:
        if previous_run_time:
            next_fire_time = previous_run_time + self.interval
        elif self.start_time:
            next_fire_time = max(now, self.start_time)
        else:
            next_fire_time = now

        if self.end_time and next_fire_time > self.end_time:
            return None
        else:
            return self.timezone.normalize(next_fire_time)

    def __getstate__(self):
        state = super().__getstate__()
        state.update({
            'version': 1,
            'interval': self.interval.total_seconds()
        })
        if self.start_time:
            state['start_time'] = datetime_to_utc_timestamp(self.start_time)
        if self.end_time:
            state['end_time'] = datetime_to_utc_timestamp(self.end_time)

        return state

    def __setstate__(self, state: Dict[str, Any]):
        if state['version'] > 1:
            raise ValueError('cannot deserialize {} definition newer than version 1 (version {} '
                             'received)'.format(self.__class__.__name__, state['version']))

        super().__setstate__(state)
        self.start_time = (datetime.fromtimestamp(state['start_time'], self.timezone)
                           if 'start_time' in state else None)
        self.end_time = (datetime.fromtimestamp(state['end_time'], self.timezone)
                         if 'end_time' in state else None)
        self.interval = timedelta(seconds=state['interval'])

    def __repr__(self):
        return ('<{self.__class__.__name__} (id={self.id!r}, task_id={self.task_id!r}, '
                'interval={self.interval!r})>'.format(self=self))

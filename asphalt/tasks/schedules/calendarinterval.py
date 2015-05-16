from datetime import timedelta, datetime, date, time
from typing import Optional, Dict, Any

from pytz.exceptions import AmbiguousTimeError, NonExistentTimeError
from typeguard import check_argument_types

from asphalt.tasks.schedules.base import BaseSchedule


class CalendarIntervalSchedule(BaseSchedule):
    """
    Runs the task on specified calendar-based intervals always at the same exact time of day.

    When calculating the next date, the ``years`` and ``months`` parameters are first added to the
    previous date while keeping the day of the month constant. This is repeated until the resulting
    date is valid. After that, the ``weeks`` and ``days`` parameters are added to that date.
    Finally, the date is combined with the given time (hour, minute, second) to form the final
    datetime.

    This means that if the ``days`` or ``weeks`` parameters are not used, the task will always be
    executed on the same day of the month at the same wall clock time, assuming the date and time
    are valid.

    If the resulting datetime is invalid due to a daylight saving forward shift, the process is
    restarted using the date part as the starting point. If instead the datetime is ambiguous due
    to a backward DST shift, the first of the two resulting datetimes is used unless that time has
    already passed, in which case the second one is used.

    If no previous run time is specified when requesting a new run time (like when starting for the
    first time or resuming after being paused), ``start_date`` is used as a reference and the next
    valid datetime equal to or later than the current time will be returned. Otherwise, the next
    valid datetime starting from the previous run time is returned, even if it's in the past.

    .. warning:: Be wary of setting a start date near the end of the month (29. – 31.) if you have
        ``months`` specified in your interval, as this will skip the months where those days do not
        exist. Likewise, setting the start date on the leap day (February 29th) and having
        ``years`` defined may cause some years to be skipped.

        Users are also discouraged from  using a time inside the target timezone's DST switching
        period (typically around 2 am) since a date could either be skipped or repeated due to the
        specified wall clock time either occurring twice or not at all.

    :param years: number of years to wait
    :param months: number of months to wait
    :param weeks: number of weeks to wait
    :param days: number of days to wait
    :param hour: hour to run the task at
    :param minute: minute to run the task at
    :param second: second to run the task at
    :param start_date: starting point for the interval calculation (defaults to current date if
        omitted)
    :param end_date: latest possible date to trigger on
    """

    __slots__ = 'years', 'months', 'weeks', 'days', 'time', 'start_date', 'end_date'

    def __init__(self, *, years: int = 0, months: int = 0, weeks: int = 0, days: int = 0,
                 hour: int = 0, minute: int = 0, second: int = 0, start_date: date = None,
                 end_date: date = None, **kwargs):
        assert check_argument_types()
        super().__init__(**kwargs)
        self.years = years
        self.months = months
        self.weeks = weeks
        self.days = days
        self.time = time(hour, minute, second)
        self.start_date = start_date or date.today()
        self.end_date = end_date

        if self.years == self.months == self.weeks == self.days == 0:
            raise ValueError('the interval must be at least 1 day long')
        if end_date and start_date > end_date:
            raise ValueError('end_date cannot be earlier than start_date')

    def get_next_run_time(self, now: datetime,
                          previous_run_time: datetime = None) -> Optional[datetime]:
        # Determine the starting point of the calculations
        today = now.date()
        if previous_run_time:
            previous_date = previous_run_time
        elif today > self.start_date:
            previous_date = self.start_date
        else:
            previous_date = None

        while True:
            if previous_date:
                year, month = previous_date.year, previous_date.month
                next_date = None
                while not next_date or next_date < today:
                    month += self.months
                    year += self.years + month // 12
                    month %= 12
                    try:
                        next_date = date(year, month, previous_date.day)
                    except ValueError:
                        pass  # Nonexistent date
                    else:
                        next_date += timedelta(self.days + self.weeks * 7)
                        break
            else:
                next_date = self.start_date

            # Don't return any date past end_date
            if self.end_date and next_date > self.end_date:
                return None

            next_time = datetime.combine(next_date, self.time)
            try:
                next_time = self.timezone.localize(next_time, is_dst=None)
            except AmbiguousTimeError:
                # This datetime occurs twice (with and without DST), so return the first of them
                # that is still equal to or later than "now". If both times have passed already,
                # move on to the next date.
                times = sorted(self.timezone.localize(next_time, is_dst=is_dst)
                               for is_dst in (False, True))
                if times[0] >= now:
                    return times[0]
                elif times[1] >= now:
                    return times[1]
            except NonExistentTimeError:
                # This datetime does not exist (the DST shift jumps over it)
                pass
            else:
                if previous_run_time or next_time >= now:
                    return next_time

            previous_date = next_date

    def __getstate__(self):
        state = super().__getstate__()
        state.update({
            'version': 1,
            'interval': (self.years, self.months, self.weeks, self.days),
            'start_date': self.start_date.toordinal()
        })
        if self.end_date:
            state['end_date'] = self.end_date.toordinal()
        if (self.time.hour, self.time.minute, self.time.second) != (0, 0, 0):
            state['time'] = self.time.hour, self.time.minute, self.time.second

        return state

    def __setstate__(self, state: Dict[str, Any]):
        if state['version'] > 1:
            raise ValueError('cannot deserialize {} definition newer than version 1 (version {} '
                             'received)'.format(self.__class__.__name__, state['version']))

        super().__setstate__(state)
        self.years, self.months, self.weeks, self.days = state['interval']
        self.time = (time(*state['time']) if 'time' in state else time())
        self.start_date = date.fromordinal(state['start_date']) if 'start_date' in state else None
        self.end_date = date.fromordinal(state['end_date']) if 'end_date' in state else None

    def __repr__(self):
        fields = 'years', 'months', 'weeks', 'days'
        interval_repr = ', '.join('%s=%d' % (attr, getattr(self, attr))
                                  for attr in fields if getattr(self, attr))
        return ('<{self.__class__.__name__} (id={self.id!r}, task_id={self.task_id!r}, '
                '{}, time={self.time})>'.format(interval_repr, self=self))

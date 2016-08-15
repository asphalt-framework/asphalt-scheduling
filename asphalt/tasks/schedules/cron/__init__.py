from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Union, Tuple, Optional, Dict, Any, List

from typeguard import check_argument_types

from asphalt.tasks.schedules.base import BaseSchedule
from asphalt.tasks.schedules.cron.fields import (
    BaseField, WeekField, DayOfMonthField, DayOfWeekField, DEFAULT_VALUES)
from asphalt.tasks.util import convert_to_datetime, datetime_ceil

FIELDS_MAP = OrderedDict([
    ('year', BaseField),
    ('month', BaseField),
    ('week', WeekField),
    ('day', DayOfMonthField),
    ('day_of_week', DayOfWeekField),
    ('hour', BaseField),
    ('minute', BaseField),
    ('second', BaseField)
])


class CronSchedule(BaseSchedule):
    """
    Runs the task when current time matches all specified time constraints, similarly to how the
    UNIX cron scheduler works.

    :param year: 4-digit year
    :param month: month (1-12)
    :param day: day of month (1-31)
    :param week: ISO week (1-53)
    :param day_of_week: number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
    :param hour: hour (0-23)
    :param minute: minute (0-59)
    :param second: second (0-59)
    :param start_time: earliest possible datetime to run the task on
    :param end_time: latest possible datetime to run the task on

    .. note:: The first weekday is always **monday**.
    """

    __slots__ = 'start_time', 'end_time', 'fields'

    def __init__(self, *, year: Union[int, str] = None, month: Union[int, str] = None,
                 day: Union[int, str] = None, week: Union[int, str] = None,
                 day_of_week: Union[int, str] = None, hour: Union[int, str] = None,
                 minute: Union[int, str] = None, second: Union[int, str] = None,
                 start_time: datetime = None, end_time: datetime = None, **kwargs):
        assert check_argument_types()
        super().__init__(**kwargs)
        self.start_time = convert_to_datetime(start_time, self.timezone)
        self.end_time = convert_to_datetime(end_time, self.timezone)
        self.fields = self._create_fields(
            year=year, month=month, week=week, day=day, day_of_week=day_of_week, hour=hour,
            minute=minute, second=second)

        if start_time and end_time and start_time > end_time:
            raise ValueError('end_time cannot be earlier than start_time')

    @staticmethod
    def _create_fields(**field_values) -> List[BaseField]:
        fields = []
        assign_defaults = False
        for key, field_class in FIELDS_MAP.items():
            if field_values.get(key) is not None:
                exprs = field_values[key]
                is_default = False
                assign_defaults = True
            else:
                exprs = DEFAULT_VALUES[key] if assign_defaults else '*'
                is_default = True

            fields.append(field_class(key, exprs, is_default))

        return fields

    def _increment_field_value(self, dateval: datetime, fieldnum: int) -> Tuple[datetime, int]:
        """
        Increment the designated field and reset all less significant fields to their minimum
        values.

        :return: a tuple containing the new date, and the number of the field that was actually
            incremented

        """
        values = {}
        i = 0
        while i < len(self.fields):
            field = self.fields[i]
            if not field.real:
                if i == fieldnum:
                    fieldnum -= 1
                    i -= 1
                else:
                    i += 1
                continue

            if i < fieldnum:
                values[field.name] = field.get_value(dateval)
                i += 1
            elif i > fieldnum:
                values[field.name] = field.get_min(dateval)
                i += 1
            else:
                value = field.get_value(dateval)
                maxval = field.get_max(dateval)
                if value == maxval:
                    fieldnum -= 1
                    i -= 1
                else:
                    values[field.name] = value + 1
                    i += 1

        difference = datetime(**values) - dateval.replace(tzinfo=None)
        return self.timezone.normalize(dateval + difference), fieldnum

    def _set_field_value(self, dateval, fieldnum, new_value):
        values = {}
        for i, field in enumerate(self.fields):
            if field.real:
                if i < fieldnum:
                    values[field.name] = field.get_value(dateval)
                elif i > fieldnum:
                    values[field.name] = field.get_min(dateval)
                else:
                    values[field.name] = new_value

        return self.timezone.localize(datetime(**values))

    def get_next_run_time(self, now: datetime,
                          previous_run_time: datetime = None) -> Optional[datetime]:
        if previous_run_time:
            start_time = previous_run_time + timedelta(seconds=1)
        elif self.start_time:
            start_time = max(now, self.start_time)
        else:
            start_time = now

        fieldnum = 0
        next_time = datetime_ceil(start_time).astimezone(self.timezone)
        while 0 <= fieldnum < len(self.fields):
            field = self.fields[fieldnum]
            curr_value = field.get_value(next_time)
            next_value = field.get_next_value(next_time)

            if next_value is None:
                # No valid value was found
                next_time, fieldnum = self._increment_field_value(next_time, fieldnum - 1)
            elif next_value > curr_value:
                # A valid, but higher than the starting value, was found
                if field.real:
                    next_time = self._set_field_value(next_time, fieldnum, next_value)
                    fieldnum += 1
                else:
                    next_time, fieldnum = self._increment_field_value(next_time, fieldnum)
            else:
                # A valid value was found, no changes necessary
                fieldnum += 1

            # Return if the date has rolled past the end date
            if self.end_time and next_time > self.end_time:
                return None

        return next_time if fieldnum >= 0 else None

    def __getstate__(self):
        state = super().__getstate__()
        state.update({
            'version': 1,
            'fields': {field.name: str(field) for field in self.fields if not field.is_default}
        })
        if self.start_time:
            state['start_time'] = self.start_time.timestamp()
        if self.end_time:
            state['end_time'] = self.end_time.timestamp()

        return state

    def __setstate__(self, state: Dict[str, Any]):
        if state['version'] > 1:
            raise ValueError('cannot deserialize {} definition newer than version 1 (version {} '
                             'received)'.format(self.__class__.__name__, state['version']))

        super().__setstate__(state)
        self.start_time = datetime.fromtimestamp(state['start_time'], self.timezone)
        self.end_time = (datetime.fromtimestamp(state['end_time'], self.timezone)
                         if 'end_time' in state else None)
        self.fields = self._create_fields(**state['fields'])

    def __repr__(self):
        options = ["{}='{}'".format(f.name, f) for f in self.fields if not f.is_default]
        return ('<{self.__class__.__name__} (id={self.id!r}, task_id={self.task_id!r}, {})>'
                .format(', '.join(options), self=self))

import re
from abc import ABCMeta, abstractmethod
from calendar import monthrange
from datetime import datetime
from typing import Union, Optional

from typeguard import check_argument_types

__all__ = ('AllExpression', 'RangeExpression', 'WeekdayRangeExpression',
           'WeekdayPositionExpression', 'LastDayOfMonthExpression')


WEEKDAYS = ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')


def as_integer(value: Union[str, int, None]) -> Optional[int]:
    assert check_argument_types()
    return int(value) if isinstance(value, str) else value


class BaseExpression(metaclass=ABCMeta):
    @abstractmethod
    def get_next_value(self, dateval: datetime, field) -> Optional[int]:
        """
        Return the next valid value for this expression, or ``None`` if there is no valid next
        value.

        """


class AllExpression(BaseExpression):
    value_re = re.compile(r'\*(?:/(?P<step>\d+))?$')

    def __init__(self, step: str = None):
        self.step = as_integer(step)
        if self.step == 0:
            raise ValueError('increment must be higher than 0')

    def get_next_value(self, dateval: datetime, field) -> Optional[int]:
        start = field.get_value(dateval)
        minval = field.get_min(dateval)
        maxval = field.get_max(dateval)
        start = max(start, minval)

        if self.step:
            distance_to_next = (self.step - (start - minval)) % self.step
            next = start + distance_to_next
        else:
            next = start

        return next if next <= maxval else None

    def __eq__(self, other):
        return isinstance(other, AllExpression) and self.step == other.step

    def __str__(self):
        if self.step:
            return '*/%d' % self.step
        return '*'


class RangeExpression(BaseExpression):
    value_re = re.compile(
        r'(?P<first>\d+)(?:-(?P<last>\d+))?(?:/(?P<step>\d+))?$')

    def __init__(self, first: Union[str, int], last: Union[str, int] = None,
                 step: Union[str, int] = None):
        self.first = as_integer(first)
        self.last = as_integer(last)
        self.step = as_integer(step)

        if self.last is None and self.step is None:
            self.last = self.first

        if self.last is not None and self.first > self.last:
            raise ValueError('the minimum value in a range must not be higher than the maximum')

    def get_next_value(self, dateval: datetime, field) -> Optional[int]:
        startval = field.get_value(dateval)
        minval = field.get_min(dateval)
        maxval = field.get_max(dateval)

        # Apply range limits
        minval = max(minval, self.first)
        maxval = min(maxval, self.last) if self.last is not None else maxval
        nextval = max(minval, startval)

        # Apply the step if defined
        if self.step:
            distance_to_next = (self.step - (nextval - minval)) % self.step
            nextval += distance_to_next

        return nextval if nextval <= maxval else None

    def __eq__(self, other):
        return (type(other) is self.__class__ and self.first == other.first and
                self.last == other.last and self.step == other.step)

    def __str__(self):
        if self.last != self.first and self.last is not None:
            range = '%d-%d' % (self.first, self.last)
        else:
            range = str(self.first)

        if self.step:
            return '%s/%d' % (range, self.step)
        return range


class WeekdayRangeExpression(RangeExpression):
    value_re = re.compile(r'(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?', re.IGNORECASE)

    def __init__(self, first: str, last: str = None):
        try:
            first = WEEKDAYS.index(first.lower())
        except ValueError:
            raise ValueError('invalid weekday name "%s"' % first) from None

        if last:
            try:
                last = WEEKDAYS.index(last.lower())
            except ValueError:
                raise ValueError('invalid weekday name "%s"' % last) from None

        super().__init__(first, last)

    def __str__(self):
        if self.last != self.first and self.last is not None:
            return '%s-%s' % (WEEKDAYS[self.first], WEEKDAYS[self.last])
        return WEEKDAYS[self.first]


class WeekdayPositionExpression(BaseExpression):
    options = ['1st', '2nd', '3rd', '4th', '5th', 'last']
    value_re = re.compile(r'(?P<option_name>%s) +(?P<weekday_name>(?:\d+|\w+))' %
                          '|'.join(options), re.IGNORECASE)

    def __init__(self, option_name: str, weekday_name: str):
        self.option_num = self.options.index(option_name.lower())
        try:
            self.weekday = WEEKDAYS.index(weekday_name.lower())
        except ValueError:
            raise ValueError('invalid weekday name "%s"' % weekday_name) from None

    def get_next_value(self, dateval: datetime, field) -> Optional[int]:
        # Figure out the weekday of the month's first day and the number of days in that month
        first_day_wday, last_day = monthrange(dateval.year, dateval.month)

        # Calculate which day of the month is the first of the target weekdays
        first_hit_day = self.weekday - first_day_wday + 1
        if first_hit_day <= 0:
            first_hit_day += 7

        # Calculate what day of the month the target weekday would be
        if self.option_num < 5:
            target_day = first_hit_day + self.option_num * 7
        else:
            target_day = first_hit_day + ((last_day - first_hit_day) // 7 * 7)

        if last_day >= target_day >= dateval.day:
            return target_day

    def __eq__(self, other):
        return (isinstance(other, WeekdayPositionExpression) and
                self.option_num == other.option_num and self.weekday == other.weekday)

    def __str__(self):
        return '%s %s' % (self.options[self.option_num], WEEKDAYS[self.weekday])


class LastDayOfMonthExpression(BaseExpression):
    value_re = re.compile('last', re.IGNORECASE)

    def get_next_value(self, dateval: datetime, field) -> Optional[int]:
        return monthrange(dateval.year, dateval.month)[1]

    def __eq__(self, other):
        return isinstance(other, LastDayOfMonthExpression)

    def __str__(self):
        return 'last'

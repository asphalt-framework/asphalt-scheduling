from calendar import monthrange
from typing import Union

from asphalt.tasks.schedules.cron.expressions import (
    AllExpression, RangeExpression, WeekdayPositionExpression, LastDayOfMonthExpression,
    WeekdayRangeExpression)


__all__ = ('MIN_VALUES', 'MAX_VALUES', 'DEFAULT_VALUES', 'BaseField', 'WeekField',
           'DayOfMonthField', 'DayOfWeekField')


MIN_VALUES = {'year': 1970, 'month': 1, 'day': 1, 'week': 1, 'day_of_week': 0, 'hour': 0,
              'minute': 0, 'second': 0}
MAX_VALUES = {'year': 2 ** 63, 'month': 12, 'day:': 31, 'week': 53, 'day_of_week': 6, 'hour': 23,
              'minute': 59, 'second': 59}
DEFAULT_VALUES = {'year': '*', 'month': 1, 'day': 1, 'week': '*', 'day_of_week': '*', 'hour': 0,
                  'minute': 0, 'second': 0}


class BaseField:
    real = True
    compilers = (AllExpression, RangeExpression)

    def __init__(self, name: str, exprs: Union[int, str], is_default: bool = False):
        self.name = name
        self.is_default = is_default
        self.expressions = [self.compile_expression(e) for e in str(exprs).split(',')]

    def get_min(self, dateval):
        return MIN_VALUES[self.name]

    def get_max(self, dateval):
        return MAX_VALUES[self.name]

    def get_value(self, dateval):
        return getattr(dateval, self.name)

    def get_next_value(self, dateval):
        values = (expr.get_next_value(dateval, self) for expr in self.expressions)
        return min((value for value in values if value is not None), default=None)

    def compile_expression(self, expr):
        for compiler in self.compilers:
            match = compiler.value_re.match(expr)
            if match:
                return compiler(**match.groupdict())

        raise ValueError('unrecognized expression "%s" for field "%s"' % (expr, self.name))

    def __eq__(self, other):
        return isinstance(self, self.__class__) and self.expressions == other.expressions

    def __str__(self):
        return ','.join(str(e) for e in self.expressions)


class WeekField(BaseField):
    real = False

    def get_value(self, dateval):
        return dateval.isocalendar()[1]


class DayOfMonthField(BaseField):
    compilers = BaseField.compilers + (WeekdayPositionExpression, LastDayOfMonthExpression)

    def get_max(self, dateval):
        return monthrange(dateval.year, dateval.month)[1]


class DayOfWeekField(BaseField):
    real = False
    compilers = BaseField.compilers + (WeekdayRangeExpression,)

    def get_value(self, dateval):
        return dateval.weekday()

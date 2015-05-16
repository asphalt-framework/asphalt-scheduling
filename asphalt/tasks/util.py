from calendar import timegm
from datetime import datetime, timedelta
from typing import Optional, Union

import pytz
from pytz.tzinfo import DstTzInfo
from typeguard import check_argument_types

T_PYTZ = Union[type(pytz.utc), DstTzInfo]


def datetime_to_utc_timestamp(timeval: Optional[datetime]) -> Optional[float]:
    """
    Convert a datetime to a numeric timestamp.

    If ``None`` is passed in, ``None`` is also returned.

    """
    if timeval is None:
        return None

    return timegm(timeval.utctimetuple()) + timeval.microsecond / 1000000


def datetime_ceil(timeval: datetime) -> datetime:
    """Round the given datetime object upwards to the nearest full second."""
    if timeval.microsecond == 0:
        return timeval

    return timeval + timedelta(seconds=1, microseconds=-timeval.microsecond)


def as_timezone(obj: Union[T_PYTZ, str]) -> Optional[T_PYTZ]:
    """
    Interpret an object as a pytz timezone.

    :param obj: either the name of a timezone (e.g. Europe/Helsinki) or a pytz timezone
    :return: a pytz timezone

    """
    assert check_argument_types()
    return pytz.timezone(obj) if isinstance(obj, str) else obj


def convert_to_datetime(value: Optional[datetime], timezone: T_PYTZ) -> Optional[datetime]:
    """
    Convert the given object to a timezone aware datetime object.

    If a timezone aware datetime object is passed, it is converted to the given timezone.
    If a naïve datetime object is passed, it is given the specified timezone.

    :param value: the datetime to convert to a timezone aware datetime
    :param timezone: timezone to interpret ``value`` in
    :return: a timezone aware datetime

    """
    assert check_argument_types()
    if value is None:
        return None
    elif value.tzinfo is None:
        return timezone.localize(value, is_dst=None)
    else:
        return value.astimezone(timezone)


def create_reference(obj) -> str:
    """Return a ``module:varname`` reference to the given object."""
    obj_name = obj.__qualname__
    if '<locals>' in obj_name:
        raise ValueError('cannot create a reproducible reference to a nested function')

    return '%s:%s' % (obj.__module__, obj_name)

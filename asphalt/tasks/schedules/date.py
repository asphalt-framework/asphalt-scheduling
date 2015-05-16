from datetime import datetime
from typing import Optional, Dict, Any

from typeguard import check_argument_types

from asphalt.tasks.schedules.base import BaseSchedule
from asphalt.tasks.util import convert_to_datetime, datetime_to_utc_timestamp


class DateSchedule(BaseSchedule):
    """
    Runs the task once on the given datetime.

    :param run_time: the date/time to run the job at
    """

    __slots__ = 'run_time'

    def __init__(self, run_time: datetime, **kwargs):
        assert check_argument_types()
        super().__init__(**kwargs)
        self.run_time = convert_to_datetime(run_time, self.timezone)

    def get_next_run_time(self, now: datetime,
                          previous_run_time: datetime = None) -> Optional[datetime]:
        return self.run_time if previous_run_time is None else None

    def __getstate__(self):
        state = super().__getstate__()
        state.update({
            'version': 1,
            'run_time': datetime_to_utc_timestamp(self.run_time)
        })
        return state

    def __setstate__(self, state: Dict[str, Any]):
        if state['version'] > 1:
            raise ValueError('cannot deserialize {} definition newer than version 1 (version {} '
                             'received)'.format(self.__class__.__name__, state['version']))

        super().__setstate__(state)
        self.run_time = datetime.fromtimestamp(state['run_time'], self.timezone)

    def __repr__(self):
        return ('<{self.__class__.__name__} (id={self.id!r}, task_id={self.task_id!r}, '
                'run_time={self.run_time!r})>'.format(self=self))

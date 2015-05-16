from abc import ABCMeta, abstractmethod
from collections import namedtuple
from typing import Callable, Any, Dict, Iterable, Tuple, Union
from uuid import UUID

from asphalt.core.concurrency import asynchronous
from asphalt.core.context import Context

Job = namedtuple('Job', ['func', 'args', 'kwargs', 'future'])


class Ticket:
    __slots__ = 'queue', 'task_id'

    def __init__(self, queue: 'TaskQueue', task_id: UUID):
        self.queue = queue
        self.task_id = task_id

    def cancel(self):
        if self.queue is None:
            raise Exception('No task queue attached')

        return self.queue.cancel(self.task_id)

    def __getstate__(self):
        return {'task_id': self.task_id}

    def __setstate(self, state):
        self.queue = None
        self.task_id = state['task_id']


class TaskQueue:
    def submit(self, func: Callable[..., Any], args: Iterable[Any],
               kwargs: Dict[str, Any]) -> Ticket:
        pass

    def schedule(self, func: Callable[..., Any], trigger: str, trigger_args: Dict[str, Any],
                 args: Iterable[Any], kwargs: Dict[str, Any]) -> Ticket:
        pass

    def cancel(self, task_id: int):
        pass


class MaxInstancesReachedError(Exception):
    def __init__(self, job):
        super().__init__(
            'Job "%s" has already reached its maximum number of instances (%d)' %
            (job.id, job.max_instances))


class ExecutorStoppedError(Exception):
    """Raised when attempting to submit a job to a stopped executor."""


class TaskExecutor(metaclass=ABCMeta):
    @abstractmethod
    @asynchronous
    def start(self, ctx: Context) -> None:
        """
        Start accepting new jobs.

        :param ctx: the current context
        """

    @abstractmethod
    @asynchronous
    def stop(self, wait_timeout: int=10) -> None:
        """
        Shut down the executor.

        Waits up to ``wait_timeout`` seconds for existing tasks to finish.

        :param wait_timeout: seconds to wait for all the tasks to finish
        """

    @abstractmethod
    @asynchronous
    def submit(self, func: Union[Callable, str], args: Tuple, kwargs: Dict[str, Any]):
        """
        Submit a job for execution.

        If the executor has been stopped, it must raise an exception to i

        :param func: the callable to execute (or a text reference to one)
        :param args: positional arguments to the callable
        :param kwargs: keyword arguments to the callable
        :raises MaxInstancesReachedError: if the maximum number of
            allowed instances for this job has been reached
        :raises ExecutorStoppedError: if the executor has been stopped

        """
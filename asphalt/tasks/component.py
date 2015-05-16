from asyncio import coroutine

from asphalt.core.component import Component
from asphalt.core.context import Context


class TaskQueueComponent(Component):
    def __init__(self, jobs: dict=None, context_property: str='scheduler', **scheduler_config):
        self.jobs = jobs or {}
        self.context_property = context_property
        self.scheduler_config = scheduler_config

    def setup_jobstores(self):
        jobstores = self.scheduler_config.get('jobstores', {})
        for name, config in jobstores.items():
            if isinstance(config, dict) and config.get_value('type', None) == 'asphalt':
                resource_type = config.pop('resource_type')
                resource_name = config.pop('resource_name', None)
                resource = yield ResourceDependency(resource_type, resource_name)
                jobstores[name] = self.create_jobstore(resource_type, resource, config)

    @staticmethod
    def create_jobstore(resource_type: str, database, config: dict):
        if resource_type == 'pymongo.database':
            from apscheduler.jobstores.mongodb import MongoDBJobStore
            config['client'] = database.client
            config['database'] = database.name
            return MongoDBJobStore(**config)
        elif resource_type == 'sqlalchemy.engine.interfaces.Connectable':
            from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
            return SQLAlchemyJobStore(engine=database, **config)

        raise ValueError('Resource type {} is not supported'.format(resource_type))

    async def start(self, ctx: Context):
        scheduler = AsyncIOScheduler(event_loop=event_loop, **self.scheduler_config)
        scheduler.start()
        for job_id, job_args in self.jobs.items():
            scheduler.add_job(id=job_id, **job_args)

        yield Resource(AsyncIOScheduler, scheduler)
        if self.context_property:
            app_ctx.add_property(ContextScope.application, self.context_property, scheduler)

        app_ctx.add_callback(lambda ctx: scheduler.shutdown())

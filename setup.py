from pathlib import Path

from setuptools import setup

setup(
    name='asphalt-tasks',
    use_scm_version={
        'version_scheme': 'post-release',
        'local_scheme': 'dirty-tag'
    },
    description='Task queue component for the Asphalt framework',
    long_description=Path(__file__).with_name('README.rst').read_text('utf-8'),
    author='Alex Grönholm',
    author_email='alex.gronholm@nextday.fi',
    url='https://github.com/asphalt-framework/asphalt-tasks',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
    license='Apache License 2.0',
    zip_safe=False,
    setup_requires=[
        'setuptools_scm >= 1.7.0'
    ],
    packages=[
        'asphalt.tasks'
    ],
    install_requires=[
        'asphalt ~= 2.0',
        'asphalt-serialization[cbor] ~= 3.0',
        'pytz',
        'tzlocal >= 1.2.2'
    ],
    extras_require={
        'testing': [
            'pytest',
            'pytest-cov',
            'pytest-catchlog',
            'pytest-asyncio >= 0.5.0'
        ]
    },
    entry_points={
        'asphalt.components': [
            'taskqueue = asphalt.tasks.component:TaskQueueComponent'
        ],
        'asphalt.tasks.schedules': [
            'calendarinterval = asphalt.tasks.schedules.calendarinterval:CalendarIntervalSchedule',
            'cron = asphalt.tasks.schedules.cron:CronSchedule',
            'date = asphalt.tasks.schedules.date:DateSchedule',
            'interval = asphalt.tasks.schedules.interval:IntervalSchedule',
        ]
    }
)

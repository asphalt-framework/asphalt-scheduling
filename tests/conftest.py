from datetime import datetime

import pytest
import pytz
from asphalt.serialization.serializers.cbor import CBORSerializer
from asphalt.serialization.serializers.json import JSONSerializer
from asphalt.serialization.serializers.pickle import PickleSerializer


@pytest.fixture(params=[JSONSerializer, CBORSerializer, PickleSerializer])
def serializer(request):
    return request.param()


@pytest.fixture(scope='session')
def timezone():
    return pytz.timezone('Europe/Berlin')


@pytest.fixture(scope='session')
def now(timezone):
    return timezone.localize(datetime(2016, 7, 23, 21, 28, 13))

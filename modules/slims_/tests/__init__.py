from functools import partial
from typing import Any
from unittest.mock import Mock

from cellophane import data
from slims.internal import Record


def add_factory():
    def add(instance, type_, fields):
        del instance, type_  # Unused
        return RecordMock(**fields)
    return add

class RecordMock(Mock):
    def __init__(self, **kwargs):

        defaults = {
            "pk": lambda *_, **__: 1337,
            "slims_api": {
                "username": "DUMMY",
                "password": "DUMMY",
                "raw_url": "DUMMY"
            },
        }

        for k, v in data.Container(kwargs | defaults).items():
            object.__setattr__(self, k, v)
        object.__setattr__(self, "json_entity", kwargs | defaults)
        super().__init__(spec_set=Record)

    def __reduce__(self):
        return partial(RecordMock, **self.json_entity), (), {}

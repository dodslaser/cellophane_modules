from cellophane import data
from attrs import define
from unittest.mock import MagicMock
from slims.slims import Record


class RecordMock(MagicMock):
    def __init__(self, **kwargs):
        super().__init__(spec=Record)
        self.__dict__.update(
            {k: data.Container(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
        )

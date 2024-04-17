"""Module for getting samples from SLIMS"""

from functools import reduce
from typing import Literal

from attrs import Attribute, define, field, fields_dict
from attrs.setters import validate
from cellophane import cfg, data, util
from slims.slims import Record, Slims

from .util import get_field, get_records


@define(slots=False)
class SlimsSample(data.Sample):
    """A sample container with SLIMS integration"""

    derived: list[tuple[Record, dict]] | None = field(default=None, on_setattr=validate)
    record: Record | None = field(default=None, on_setattr=validate)
    # FIXME: Rename to slims_state as a property which checks `self.failed`
    state: Literal["novel", "running", "complete", "error"] = field(
        default="novel", on_setattr=validate
    )
    _connection: Slims | None = field(default=None, init=False)

    @classmethod
    def from_record(cls: data.Sample, record: Record, config: cfg.Config, **kwargs):
        """Create a sample from a SLIMS fastq record"""

        _sample = cls(
            id=record.cntn_id.value,
            state=kwargs.pop("state", "novel"),
            **kwargs,
        )

        _map = config.slims.get("map", {})
        _keys = {*util.map_nested_keys(_map)} - {(key,) for key in kwargs}
        try:
            for key in _keys:
                _field = reduce(lambda x, y: x[y], key, _map)
                _value = get_field(record, _field)
                if key[0] == "meta":
                    _sample.meta[key[1:]] = _value
                elif key[0] in fields_dict(cls):
                    node = reduce(getattr, key[:-1], _sample)
                    setattr(node, key[-1], _value)
                else:
                    raise KeyError
        except Exception as exc:
            raise KeyError(
                f"Unable to map '{'.'.join(key)}' to field in sample"
            ) from exc

        _sample.record = record
        return _sample

    def update_derived(
        self,
        config: cfg.Config,
    ):
        """Update/add derived records for the sample"""
        if not self.derived:
            self.derived = [(None, key_map) for key_map in config.slims.derive]
        if self.record:
            for idx, (record, key_map) in enumerate(self.derived):
                fields = {
                    key: (
                        value.format(sample=self) if isinstance(value, str) else value
                    )
                    for key, value in key_map.items()
                } | {
                    "cntn_id": self.record.cntn_id.value,
                    "cntn_fk_originalContent": self.pk,
                    "cntn_fk_user": config.slims.username,
                }
                if record:
                    self.derived[idx] = (record.update(fields), key_map)
                elif self.connection:
                    self.derived[idx] = (
                        self.connection.add("Content", fields),
                        key_map,
                    )

    @derived.validator
    def _validate_derived(
        self,
        attribute: Attribute,
        value: list[tuple[Record | None, dict]] | None,
    ):
        if value is not None:
            if not isinstance(value, list):
                raise ValueError(
                    f"Expected 'None|list' for '{attribute.name}', got '{value}'"
                )
            elif not all(
                isinstance(v, tuple)
                and len(v) == 2
                and (isinstance(v[0], Record) or v[0] is None)
                and isinstance(v[1], dict)
                for v in value
            ):
                raise ValueError(
                    "Expected 'list[tuple[Record|None, dict]' "
                    f"for '{attribute.name}', got {value}"
                )

    @record.validator
    def _validate_record(self, attribute: Attribute, value: Record | None):
        if not (value is None or isinstance(value, Record)):
            raise ValueError(
                "Expected 'NoneType' or 'Record' for "
                f"'{attribute.name}', got '{value}'"
            )

    @state.validator
    def _validate_state(
        self,
        attribute: Attribute,
        value: Literal[
            "novel",
            "running",
            "complete",
            "error",
        ],
    ):
        if value not in ["novel", "running", "complete", "error"]:
            raise ValueError(
                f"'{attribute.name}' must be one of "
                f"'novel', 'running', 'complete', 'error', got '{value}'"
            )

    @property
    def pk(self):
        """Get the primary key of the record"""
        return self.record.pk() if self.record is not None else None

    @property
    def connection(self) -> Slims | None:
        """Get a connection to SLIMS from the record"""
        if self._connection is None and self.record:
            self._connection = Slims(
                "cellophane",
                url=self.record.slims_api.raw_url,
                username=self.record.slims_api.username,
                password=self.record.slims_api.password,
            )

        return self._connection

    def __reduce__(self) -> str | tuple:
        """Remove open connection before pickle"""
        self._connection = None
        return data.Sample.__reduce__(self)


@data.Sample.merge.register("record")
def _(this, _):
    return this


@data.Sample.merge.register("_connection")
def _(*_):
    return None


@data.Sample.merge.register("state")
def _(this, that):
    return "error" if any(s != "complete" for s in (this, that)) else "complete"


@data.Sample.merge.register("derived")
def _(this, that):
    if not this or that is None:
        return (this or []) + (that or [])


class SlimsSamples(data.Samples):
    """A list of sample containers with SLIMS integration"""

    @classmethod
    def from_records(
        cls,
        records: list[Record],
        config: cfg.Config,
    ) -> "SlimsSamples":
        """Get samples from SLIMS records"""
        return cls(
            [
                cls.sample_class.from_record(  # pylint: disable=no-member
                    record=record, config=config
                )
                for record in records
            ]
        )

    @classmethod
    def from_criteria(
        cls,
        criteria: str,
        config: cfg.Config,
        connection: Slims | None = None,
        **kwargs,
    ) -> "SlimsSamples":
        """Get samples from SLIMS records"""
        _connection = connection or Slims(
            name=__package__,
            url=config.slims.url,
            username=config.slims.username,
            password=config.slims.password,
        )
        records = get_records(
            string_criteria=criteria,
            connection=_connection,
            **kwargs,
        )

        return cls.from_records(records, config)

    def update_derived(
        self,
        config: cfg.Config,
    ) -> None:
        """Update derived records in SLIMS"""
        for sample in self:
            sample.update_derived(config)

    def set_state(self, value: Literal["novel", "running", "complete", "error"]):
        """Set the state of the samples"""
        for sample in self:
            sample.state = value

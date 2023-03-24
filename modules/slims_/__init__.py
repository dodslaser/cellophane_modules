"""Module for getting samples from SLIMS"""

from copy import deepcopy
from functools import cached_property, reduce
from json import loads
from logging import LoggerAdapter
from time import time
from typing import Optional, Any
from collections import UserList, UserDict
import re

from humanfriendly import parse_timespan
from slims.criteria import (
    Criterion,
    conjunction,
    disjunction,
    ends_with,
    starts_with,
    contains,
    is_one_of,
    equals,
    equals_ignore_case,
    greater_than,
    less_than,
    between_inclusive,
    is_not,
)
from slims.slims import Record, Slims

from cellophane import cfg, data, modules


def _parse_bool(string: str) -> list[str]:
    """
    Split string on "and"/"or" but not within parentheses

    >>> _parse_bool("a is x and (b is y or c is d) or g is h")
    ['a is x', 'and', 'b is y or c is d', 'or', 'g is h']
    """
    string = " ".join(string.split())
    parts: list[str] = []
    part = ""
    depth = 0
    while string:
        if string[0] == "(":
            if depth > 0:
                part += string[0]
            depth += 1
        elif string[0] == ")":
            if depth > 1:
                part += string[0]
            depth -= 1
        elif string[1:4] == "and" and depth == 0:
            parts.append(part)
            parts.append("and")
            string = string[4:]
            part = ""
        elif string[1:3] == "or" and depth == 0:
            parts.append(part)
            parts.append("or")
            string = string[3:]
            part = ""
        else:
            part += string[0]
            if len(string) == 1:
                parts.append(part)
        string = string[1:]

    while len(parts) == 1 and any(w in parts[0] for w in [" and ", " or ", "(", ")"]):
        parts = _parse_bool(parts[0])

    return parts


def _parse_criteria(criteria: str) -> Criterion:
    """Parse criteria"""

    _criteria = _parse_bool(criteria)
    if len(_criteria) == 1:
        _criteria = _criteria[0].split(" ")

    match _criteria:
        case [a, "and", b]:
            return conjunction().add(_parse_criteria(a)).add(_parse_criteria(b))
        case [a, "or", b]:
            return disjunction().add(_parse_criteria(a)).add(_parse_criteria(b))
        case [field, *_] if not field.startswith("cntn_"):
            raise ValueError(f"Invalid field: {field}")

        case [field, "equals", value]:
            return equals(field, value)
        case [field, "not_equals", value]:
            return is_not(equals(field, value))
        case [field, "is_one_of", *values]:
            return is_one_of(field, values)

        case [field, "equals_ignore_case", value]:
            return equals_ignore_case(field, value)
        case [field, "not_equals_ignore_case ", value]:
            return is_not(equals_ignore_case(field, value))

        case [field, "contains", value]:
            return contains(field, value)
        case [field, "not_contains ", value]:
            return is_not(contains(field, value))

        case [field, "starts_with", value]:
            return starts_with(field, value)
        case [field, "not_starts_with", value]:
            return is_not(starts_with(field, value))

        case [field, "ends_with", value]:
            return ends_with(field, value)
        case [field, "not_ends_with", value]:
            return is_not(ends_with(field, value))

        case [field, "between_inclusive", *values]:
            return between_inclusive(field, *values)
        case [field, "not_between_inclusive", *values]:
            return is_not(between_inclusive(field, *values))

        case [field, "greater_than", value]:
            return greater_than(field, value)
        case [field, "less_than", value]:
            return less_than(field, value)
        case _:
            raise ValueError(f"Invalid criteria: {criteria}")


def _get_field(record: Record, field: str, default=None) -> Any:
    """Get a field from SLIMS record"""
    try:
        if field.startswith("json:"):
            _field, *_key = re.split(r"\.|(\[[0-9]*\])", field[5:])
            _key = [int(k.strip("[]")) if k.startswith("[") else k for k in _key if k]
            _json = loads(record.__dict__[_field].value)
            return reduce(lambda x, y: x[y], _key, _json)
        else:
            return record.__getattribute__(field).value
    except AttributeError:
        return default
    except KeyError:
        return default

def get_records(
    *args: Criterion,
    connection: Slims,
    slims_id: Optional[str | list[str]] = None,
    max_age: Optional[int | str] = None,
    content_type: Optional[int | list[int]] = None,
    **kwargs: str | int | list[str | int],
) -> list[Record]:
    """Get records from SLIMS"""

    criteria = conjunction()

    match slims_id:
        case str():
            criteria = criteria.add(equals("cntn_id", slims_id))
        case [*ids]:
            criteria = criteria.add(is_one_of("cntn_id", ids))
        case _ if slims_id is not None:
            raise TypeError(f"Invalid type for id: {type(slims_id)}")

    match max_age:
        case int() | str():
            min_mtime = int(time() - parse_timespan(str(max_age))) * 1e3
            criteria = criteria.add(greater_than("cntn_modifiedOn", min_mtime))
        case _ if max_age is not None:
            raise TypeError(f"Expected int or str, got {type(max_age)}")

    match content_type:
        case None:
            pass
        case int():
            criteria = criteria.add(equals("cntn_fk_contentType", content_type))
        case [*_, int()]:
            criteria = criteria.add(is_one_of("cntn_fk_contentType", content_type))
        case _:
            raise TypeError(f"Expected int(s), got {type(content_type)}")

    for key, value in kwargs.items():
        criteria = criteria.add(
            is_one_of(key, [value] if isinstance(value, int | str) else value)
        )

    for arg in args:
        criteria = criteria.add(arg)

    return connection.fetch("Content", criteria)


def get_derived_records(
    *args,
    connection: Slims,
    derived_from: Record | list[Record],
    content_type: Optional[int | list[int]] = None,
    **kwargs,
) -> dict[Record, list[Record]]:
    """Get derived records from SLIMS"""
    match derived_from:
        case record if isinstance(record, Record):
            original = {record.pk(): record}
        case [*records] if all(isinstance(r, Record) for r in records):
            original = {r.pk(): r for r in records}
        case _:
            raise TypeError(f"Expected Record(s), got {derived_from}")

    criterion = is_one_of("cntn_fk_originalContent", [*original])
    records = get_records(
        connection,
        criterion,
        *args,
        content_type=content_type,
        **kwargs,
    )

    derived = {
        o: [r for r in records if r.cntn_cstm_originalContent.value == pk]
        for pk, o in original.items()
    }

    return derived


class SlimsSample(UserDict):
    """A sample container with SLIMS integration"""

    record: Optional[Record]
    bioinformatics: Optional[Record]

    @classmethod
    def from_record(cls, record: Record, config: cfg.Config, **kwargs):
        """Create a sample from a SLIMS fastq record"""
        return cls(
            id=record.cntn_id.value,
            bioinformatics=None,
            record=record,
            files=_get_field(record, config.slims.files_field, []),
            backup=_get_field(record, config.slims.backup_field, []),
            **{
                key: _get_field(record, field)
                for key, field in config.slims.extra_fields.items()
            },
            **kwargs,
        )

    def add_bioinformatics(
        self,
        content_type: int,
        state_field: str,
        additional: dict = {},
    ):
        """Add a bioinformatics record to the sample"""

        if self.bioinformatics is None and self._connection is not None:
            self.bioinformatics = self._connection.add(
                "Content",
                {
                    "cntn_id": self.record.cntn_id.value,
                    "cntn_fk_contentType": content_type,
                    "cntn_fk_originalContent": self.record.pk(),
                    state_field: "novel",
                }
                | additional,
            )

    def set_bioinformatics_state(self, state, state_field):
        """Set the bioinformatics state"""

        match state:
            case "running" | "complete" | "error":
                if self.bioinformatics is not None:
                    self.bioinformatics = self.bioinformatics.update(
                        {state_field: state}
                    )
            case _:
                raise ValueError(f"Invalid state: {state}")

    @cached_property
    def _connection(self) -> Optional[Slims]:
        """Get a connection to SLIMS from the record"""

        return (
            Slims(
                "cellophane",
                url=self.record.slims_api.raw_url,
                username=self.record.slims_api.username,
                password=self.record.slims_api.password,
            )
            if self.record is not None
            else None
        )

    def __reduce__(self) -> str | tuple:
        """Remove open connection before pickle"""

        if hasattr(self, "_connection"):
            delattr(self, "_connection")
        return super().__reduce__()


class SlimsSamples(UserList, data.Mixin, sample_mixin=SlimsSample):
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
                cls.sample_class.from_record(record=record, config=config)
                for record in records
            ]
        )

    def add_bioinformatics(self, analysis: int) -> None:
        """Add bioinformatics content to SLIMS samples"""
        for sample in self:
            sample.add_bioinformatics(analysis)

    def set_bioinformatics_state(self, state: str) -> None:
        """Update bioinformatics state in SLIMS"""
        match state:
            case "running" | "complete" | "error":
                for sample in self:
                    sample.set_bioinformatics_state(state)
            case _:
                raise ValueError(f"Invalid state: {state}")


@modules.pre_hook(label="SLIMS Fetch", priority=0)
def slims_samples(
    samples: data.Samples,
    config: cfg.Config,
    logger: LoggerAdapter,
    **_,
) -> Optional[SlimsSamples]:
    """Load novel samples from SLIMS."""
    if "slims" in config:
        slims_connection = Slims(
            name=__package__,
            url=config.slims.url,
            username=config.slims.username,
            password=config.slims.password,
        )

        if samples:
            logger.debug("Augmenting existing samples")
            records = get_records(
                _parse_criteria(config.slims.criteria),
                connection=slims_connection,
                slims_id=[s.id for s in samples],
            )
        elif config.slims.get("ids", None):
            logger.debug("Fetching samples by ID")
            records = get_records(
                _parse_criteria(config.slims.criteria),
                connection=slims_connection,
                slims_id=config.slims.ids,
            )
        elif config.slims.criteria is not None:
            logger.debug(f"Fetching samples from the last {config.slims.novel_max_age}")
            records = get_records(
                _parse_criteria(config.slims.criteria),
                connection=slims_connection,
                max_age=config.slims.novel_max_age,
            )
        else:
            logger.error("No way to fetch samples from SLIMS")
            return None

        if config.slims.derived:
            records = [
                record
                for original in get_derived_records(
                    _parse_criteria(config.slims.derived_criteria),
                    connection=slims_connection,
                    derived_from=records,
                ).values()
                for record in original
            ]

        if "derived_bioinfo" in config.slims:
            bioinfo = get_derived_records(
                _parse_criteria(config.slims.derived_criteria),
                connection=slims_connection,
                derived_from=records,
                content_type=config.slims.derived_bioinfo.content_type,
            )
            for sample in bioinfo.keys():
                logger.info(
                    f"Skipping {sample.id} as it already has bioinformatics in SLIMS "
                    "(use --slims_derived_bioinfo_ignore_existing) to override"
                )
            records = [
                record
                for record in records
                if config.slims.derived_bioinfo.ignore_existing
                or record.pk() not in [b.pk() for b in bioinfo.keys()]
            ]

        slims_samples = samples.from_records(records)

        if samples:
            for sample in samples:
                _ss = [s for s in slims_samples if s.id == sample.id]
                if len(_ss) > 1 and "pk" in sample:
                    _ss = [s for s in _ss if s.pk == sample.pk]
                elif len(_ss) > 1 and "run" in sample:
                    _ss = [s for s in _ss if s.run == sample.run]
                if len(_ss) > 1:
                    logger.warning(f"Multiple SLIMS samples found for {sample.id}")
                elif len(_ss) == 0:
                    logger.warning(f"SLIMS sample not found for {sample.id}")
                else:
                    if sample.files == [None]:
                        sample.pop("files")
                    _data = {**_ss[0]} | {**sample}

                    sample = sample.__class__(
                        id=_data.pop("id"),
                        record=_data.pop("record", None),
                        **deepcopy(_data),
                    )
            return samples
        else:
            return slims_samples

    else:
        logger.warning("No SLIMS connection configured")
        return None


@modules.pre_hook(label="SLIMS Add")
def slims_bioinformatics(
    samples: data.Samples,
    config: cfg.Config,
    logger: LoggerAdapter,
    **_,
) -> None:
    """Load novel samples from SLIMS."""
    if config.slims.dry_run:
        logger.debug("Dry run - Not adding bioinformatics")
    elif isinstance(samples, SlimsSamples):
        logger.info("Adding bioinformatics content")
        samples.add_bioinformatics(config.slims.analysis_pk)
        samples.set_bioinformatics_state("running")
    else:
        logger.debug("Samples not from SLIMS")


@modules.post_hook(label="SLIMS Update")
def slims_update(
    config: cfg.Config,
    samples: SlimsSamples,
    logger: LoggerAdapter,
    **_,
) -> None:
    """Update SLIMS samples with bioinformatics content."""

    if config.slims.dry_run:
        logger.info("Dry run - Not updating SLIMS")
    elif isinstance(samples, SlimsSamples):
        logger.info("Updating bioinformatics")
        pks = {s.record.pk() for s in samples if s.record.pk() is not None}
        collect = {pk: [*filter(lambda s: s.record.pk() == pk, samples)] for pk in pks}

        for pk_samples in collect.values():
            if all(s.complete for s in pk_samples):
                pk_samples[0].set_bioinformatics_state("complete")
            else:
                pk_samples[0].set_bioinformatics_state("error")
    else:
        logger.info("No SLIMS samples to update")

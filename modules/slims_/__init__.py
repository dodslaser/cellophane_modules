"""Module for getting samples from SLIMS"""

from copy import deepcopy
from functools import cached_property
from logging import LoggerAdapter
from typing import Literal

from attrs import define, field
from cellophane import cfg, data, modules
from slims.slims import Record, Slims

from .src.util import get_field, get_records


@define(slots=False, init=False)
class SlimsSample(data.Sample):
    """A sample container with SLIMS integration"""

    derived: list[tuple[Record, dict]] | None = field(default=None)
    record: Record | None = field(default=None)
    state: Literal["novel", "running", "complete", "error"] = field(default="novel")

    @classmethod
    def from_record(cls, record: Record, config: cfg.Config, **kwargs):
        """Create a sample from a SLIMS fastq record"""

        _sample = cls(
            id=record.cntn_id.value,
            state="novel",
            **{
                key: get_field(record, field)
                for key, field in config.slims.map[0].items()
            },
            **kwargs,
        )  # type: ignore[call-arg]
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
                fields = {key: value.format(**self) for key, value in key_map.items()}
                fields |= {
                    "cntn_id": self.record.cntn_id.value,
                    "cntn_fk_originalContent": self.record.pk(),
                    "cntn_fk_user": config.slims.username,
                }
                if record:
                    self.derived[idx] = (record.update(fields), key_map)
                elif self._connection:
                    self.derived[idx] = (
                        self._connection.add("Content", fields),
                        key_map,
                    )

    @derived.validator
    def validate_derived(
        self,
        attribute: str,
        value: list[tuple[Record | None, dict]] | None,
    ):
        if not (value is None or isinstance(value, list)):
            raise ValueError(f"Expected 'None|list', got {value}")
        elif value is not None and not all(
            isinstance(v, tuple)
            and len(v) == 2
            and (isinstance(v[0], Record) or v[0] is None)
            and isinstance(v[1], dict)
            for v in value
        ):
            raise ValueError(
                f"Expected 'list[tuple[Record|None, dict]' for {attribute}, got {value}"
            )

    @record.validator
    def validate_record(self, attribute: str, value: Record | None):
        if not (value is None or isinstance(value, Record)):
            raise ValueError(
                f"Expected 'NoneType' or 'Record' for {attribute}, got {value}"
            )

    @state.validator
    def validate_state(
        self, attribute: str, value: Literal["novel", "running", "complete", "error"]
    ):
        """Set the state of the sample"""
        if value not in ["novel", "running", "complete", "error"]:
            raise ValueError(f"Invalid value for {attribute}: {value}")

    @property
    def pk(self):
        return self.record.pk()

    @cached_property
    def _connection(self) -> Slims | None:
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
                cls.sample_class.from_record(record=record, config=config)
                for record in records
            ]
        )

    def update_derived(
        self,
        config: cfg.Config,
    ) -> None:
        """Update derived records in SLIMS"""
        for sample in self:
            sample.update_derived(config)

    def set_state(self, value: Literal["novel", "running", "complete", "error"]):
        """Set the state of the samples"""
        if value not in ["novel", "running", "complete", "error"]:
            raise ValueError(f"Invalid state: {value}")
        else:
            for sample in self:
                sample.state = value


@modules.pre_hook(label="SLIMS Fetch", before=["hcp_fetch", "slims_bioinformatics"])
def slims_fetch(
    samples: data.Samples,
    config: cfg.Config,
    logger: LoggerAdapter,
    **_,
) -> SlimsSamples | None:
    """Load novel samples from SLIMS."""
    if "slims" in config:
        slims_connection = Slims(
            name=__package__,
            url=config.slims.url,
            username=config.slims.username,
            password=config.slims.password,
        )

        slims_ids: list[str] | None = None
        max_age: str | None = None
        if samples:
            logger.info("Augmenting existing samples with info from SLIMS")
            slims_ids = [s.id for s in samples]

        elif config.slims.id:
            logger.info("Fetching samples from SLIMS by ID")
            slims_ids = config.slims.id

        else:
            logger.info(f"Fetching samples from the last {config.slims.novel_max_age}")
            max_age = config.slims.novel_max_age

        records = get_records(
            string_criteria=config.slims.find_criteria,
            connection=slims_connection,
            slims_id=slims_ids,
            max_age=max_age,
            unrestrict_parents=config.slims.unrestrict_parents,
        )

        if samples and records:
            for idx, sample in enumerate(samples):
                match = [
                    m
                    for m in samples.from_records(records, config)
                    if m.id == sample.id
                ]
                common_keys = set([k for s in match for k in s]) & set(sample.keys())
                for key in common_keys:
                    _match = []
                    for match_sample in match:
                        if (
                            (m_value := match_sample[key])
                            and (s_value := sample[key])
                            and s_value == m_value
                        ):
                            _match.append(match_sample)
                    match = _match

                if len(match) > 1:
                    logger.warning(f"Multiple SLIMS samples found for {sample.id}")
                elif len(match) == 0:
                    logger.warning(f"SLIMS sample not found for {sample.id}")
                else:
                    if sample.files is None:
                        sample.pop("files")
                    _data = {key: sample[key] or match[0][key] for key in sample.keys()}
                    samples[idx] = sample.__class__(**deepcopy(_data))

            return samples

        elif records:
            if config.slims.check:
                logger.info("Checking SLIMS for completed samples")
                check = get_records(
                    string_criteria=config.slims.check_criteria,
                    connection=slims_connection,
                    derived_from=records,
                )

                original_ids = [r.cntn_id.value for r in records]
                records = [
                    record
                    for record in records
                    if record.pk()
                    not in [b.cntn_fk_originalContent.value for b in check]
                ]

                for sid in set(original_ids) - set([r.cntn_id.value for r in records]):
                    logger.info(f"Found completed bioinformatics for {sid}")

            return samples.from_records(records, config)

        else:
            logger.warning("No SLIMS samples found")
            return None

    else:
        logger.warning("No SLIMS connection configured")
        return None


@modules.pre_hook(label="SLIMS Derive", after=["slims_fetch"])
def slims_derive(
    samples: SlimsSamples,
    config: cfg.Config,
    logger: LoggerAdapter,
    **_,
) -> SlimsSamples:
    """Add derived content to SLIMS samples"""
    if config.slims.dry_run:
        logger.debug("Dry run - Not adding derived records")
    elif config.slims.derive and samples:
        logger.info("Creating derived records")
        samples.update_derived(config)
    return samples


@modules.pre_hook(label="SLIMS Mark Running", after="all")
def slims_running(
    samples: SlimsSamples,
    config: cfg.Config,
    logger: LoggerAdapter,
    **_,
) -> SlimsSamples:
    """Add derived content to SLIMS samples"""
    samples.set_state("running")
    if not config.slims.dry_run:
        logger.info("Setting SLIMS samples to running")
        samples.update_derived(config)
    return samples


@modules.post_hook(label="SLIMS Update Derived")
def slims_update(
    config: cfg.Config,
    samples: SlimsSamples,
    logger: LoggerAdapter,
    **_,
) -> None:
    """Update SLIMS samples and derived records."""
    if config.slims.dry_run:
        logger.info("Dry run - Not updating SLIMS")
        return

    complete = samples.__class__.from_records(
        records=[*{s.pk: s.record for s in samples.complete}.values()], config=config
    )
    failed = samples.__class__.from_records(
        records=[*{s.pk: s.record for s in samples.failed}.values()], config=config
    )

    if not complete and not failed:
        logger.info("No samples to update")

    if complete:
        logger.info(f"Marking {len(complete)} samples as complete")
        for sample in complete:
            logger.debug(f"Marking {sample.id} as complete")
        complete.set_state("complete")
        complete.update_derived(config)

    if failed:
        logger.warning(f"Marking {len(failed)} samples as failed")
        for sample in failed:
            logger.debug(f"Marking {sample.id} as failed")
        failed.set_state("error")
        failed.update_derived(config)

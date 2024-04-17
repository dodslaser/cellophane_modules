"""Module for getting samples from SLIMS"""

from copy import deepcopy
from logging import LoggerAdapter

from attrs import fields_dict
from cellophane import Samples, cfg, data, modules
from slims.slims import Slims

from .mixins import SlimsSamples
from .util import get_records


@modules.pre_hook(label="SLIMS Fetch", before=["hcp_fetch", "slims_derive"])
def slims_fetch(
    samples: data.Samples,
    config: cfg.Config,
    logger: LoggerAdapter,
    **_,
) -> SlimsSamples | None:
    """Load novel samples from SLIMS."""
    if any(w not in config.slims for w in ["url", "username", "password"]):
        logger.warning("SLIMS connection not configured")
        return None

    slims_connection = Slims(
        name=__package__,
        url=config.slims.url,
        username=config.slims.username,
        password=config.slims.password,
    )

    slims_ids: list[str] | None = None
    if samples:
        logger.info("Augmenting existing samples with info from SLIMS")
        slims_ids = [s.id for s in samples]

    elif config.slims.get("id"):
        logger.info("Fetching samples from SLIMS by ID")
        slims_ids = config.slims.id

    records = get_records(
        string_criteria=config.slims.find_criteria,
        connection=slims_connection,
        slims_id=slims_ids,
        max_age=config.slims.novel_max_age,
        unrestrict_parents=config.slims.unrestrict_parents,
    )

    if not records:
        logger.warning("No SLIMS samples found")
        return None

    if samples:
        slims_samples = samples.from_records(records, config)
        for sample in deepcopy(samples):
            matches = [
                slims_sample
                for slims_sample in slims_samples
                if sample.id == slims_sample.id
                and all(
                    sample.meta.get(k) == slims_sample.meta.get(k) for k in sample.meta
                )
            ]

            if not matches:
                logger.warning(f"Unable to find SLIMS record for {sample.id}")
                continue

            elif len(matches) > 1 and not config.slims.allow_duplicates:
                logger.warning(f"Found multiple SLIMS records for {sample.id}")
                continue

            else:
                logger.debug(f"Found {len(matches)} SLIMS records(s) for {sample.id}")

            sample_kwargs = {
                k: v
                for k, f in fields_dict(sample.__class__).items()
                if k not in ("id", "state", "uuid", "record", "derived")
                and (v := getattr(sample, k))
                != (getattr(f.default, "factory", lambda: f.default)())  # pylint: disable=cell-var-from-loop
            }
            for match in matches:
                match_sample = sample.from_record(match.record, config, **sample_kwargs)
                samples.insert(samples.index(sample), match_sample)

            samples.remove(sample)
        return samples

    else:
        logger.debug(f"Found {len(records)} SLIMS samples")
        if "check_criteria" not in config.slims:
            logger.info("No SLIMS check criteria - Skipping check")
            return samples.from_records(records, config)

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
            if record.pk() not in [b.cntn_fk_originalContent.value for b in check]
        ]

        completed = set(original_ids) - {r.cntn_id.value for r in records}
        logger.info(f"Skipping {len(completed)} previously completed samples")
        for sid in completed:
            logger.debug(f"{sid} already completed - Skipping")

        return samples.from_records(records, config)


@modules.pre_hook(label="SLIMS Derive", after=["slims_fetch"])
def slims_derive(
    samples: SlimsSamples,
    config: cfg.Config,
    logger: LoggerAdapter,
    **_,
) -> SlimsSamples:
    """Add derived content to SLIMS samples"""
    if "derive" not in config.slims:
        return samples
    elif config.slims.dry_run:
        logger.debug("Dry run - Not adding derived records")
        return samples

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
    if "derive" not in config.slims:
        return samples
    elif config.slims.dry_run:
        logger.debug("Dry run - Not updating SLIMS")
        return samples

    logger.info("Setting SLIMS samples to running")
    samples.update_derived(config)
    return samples


@modules.post_hook(label="SLIMS Update Derived")
def slims_update(
    config: cfg.Config,
    samples: SlimsSamples,
    logger: LoggerAdapter,
    **_,
) -> Samples:
    """Update SLIMS samples and derived records."""
    if "derive" not in config.slims:
        return samples
    elif config.slims.dry_run:
        logger.info("Dry run - Not updating SLIMS")
        return samples

    if complete := samples.complete:
        logger.info(f"Marking {len(complete)} samples as complete")
        for sample in complete:
            logger.debug(f"Marking {sample.id} as complete")
        complete.set_state("complete")
        complete.update_derived(config)

    if failed := samples.failed:
        logger.warning(f"Marking {len(failed)} samples as failed")
        for sample in failed:
            logger.debug(f"Marking {sample.id} as failed")
        failed.set_state("error")
        failed.update_derived(config)

    return samples

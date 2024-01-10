"""Module for fetching files from HCP."""

import logging
import multiprocessing as mp
import sys
from logging import LoggerAdapter
from pathlib import Path

from attrs import Attribute, define, field
from cellophane import cfg, data, logs, modules
from mpire import WorkerPool
from mpire.async_result import AsyncResult
from NGPIris.hcp import HCPManager


def _fetch(
    log_queue: mp.Queue = None,
    *,
    credentials: Path,
    local_path: Path,
    remote_key: str,
) -> str:
    sys.stdout = open("/dev/null", "w", encoding="utf-8")
    sys.stderr = open("/dev/null", "w", encoding="utf-8")
    logs.setup_queue_logging(log_queue)
    logger = logging.LoggerAdapter(logging.getLogger(), {"label": "HCP Fetch"})
    if local_path.exists():
        return "local", local_path

    hcpm = HCPManager(
        credentials_path=credentials,
        bucket="data",  # FIXME: make this configurable
    )

    logger.info(f"Fetching {remote_key} from HCP")
    hcpm.download_file(
        remote_key,
        local_path=str(local_path),
        callback=False,
        force=True,
    )
    return "hcp", local_path


def _callback(sample, f_idx, logger):
    def inner(result: AsyncResult):
        location, local_path = result
        if location != "local":
            logger.info(f"Fetched {local_path.name} from hcp")
        else:
            logger.debug(f"Found {local_path.name} locally")

        sample.files.insert(f_idx, local_path)

    return inner


def _error_callback(sample, logger):
    def inner(exception: Exception):
        logger.error(f"Failed to fetch backup for {sample.id} ({exception})")
        sample.files = []

    return inner


@define(slots=False, init=False)
class HCPSample(data.Sample):
    """Sample with HCP backup."""

    hcp_remote_keys: set[str] | None = field(
        default=None,
        kw_only=True,
        converter=lambda value: None if value is None else set(value),
    )

    @hcp_remote_keys.validator
    def _validate_hcp_remote_keys(
        self,
        attribute: Attribute,
        value: set[str] | None,
    ) -> None:
        if value is None:
            return
        elif any(not isinstance(v, str) for v in value):
            raise TypeError(f"Invalid type for value in {attribute.name}: {value}")
        elif len(value) != len(self.files):
            raise ValueError(
                f"Length mismatch between {attribute.name} and files: "
                f"{len(value)} != {len(self.files)}"
            )


@data.Sample.merge.register("backup")
def _(this, that):
    return this | that


@modules.pre_hook(label="HCP", after=["slims_fetch"])
def hcp_fetch(
    samples: data.Samples,
    config: cfg.Config,
    logger: LoggerAdapter,
    log_queue: mp.Queue,
    **_,
) -> data.Samples:
    """Fetch files from HCP."""
    if any(k not in config.get("hcp", {}) for k in ["credentials", "fastq_temp"]):
        logger.warning("HCP not configured")
        return samples

    with WorkerPool(
        n_jobs=config.hcp.parallel,
        use_dill=True,
        shared_objects=log_queue,
    ) as pool:
        for sample in samples:
            if all(Path(f).exists() for f in sample.files):
                logger.info(f"All files for {sample.id} found locally")
                continue

            sample.files = []
            if sample.hcp_remote_keys is None:
                logger.warning(f"No backup for {sample.id}")
                continue

            for f_idx, remote_key in enumerate(sample.hcp_remote_keys):
                pool.apply_async(
                    _fetch,
                    kwargs={
                        "credentials": config.hcp.credentials,
                        "local_path": config.hcp.fastq_temp / Path(remote_key).name,
                        "remote_key": remote_key,
                    },
                    callback=_callback(sample, f_idx, logger),
                    error_callback=_error_callback(sample, logger),
                )

        pool.join()

    return samples

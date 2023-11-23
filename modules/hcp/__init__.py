"""Module for fetching files from HCP."""

import sys
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from logging import LoggerAdapter
from pathlib import Path

from attrs import Attribute, define, field
from cellophane import cfg, data, modules
from NGPIris.hcp import HCPManager


def _fetch(
    credentials: Path,
    local_path: Path,
    remote_key: str,
) -> str:
    sys.stdout = open("/dev/null", "w", encoding="utf-8")
    sys.stderr = open("/dev/null", "w", encoding="utf-8")
    if local_path.exists():
        return "cache"

    hcpm = HCPManager(
        credentials_path=credentials,
        bucket="data",  # FIXME: make this configurable
    )

    hcpm.download_file(
        remote_key,
        local_path=str(local_path),
        callback=False,
        force=True,
    )

    return "hcp"


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
    **_,
) -> data.Samples:
    """Fetch files from HCP."""
    if any(k not in config.get("hcp", {}) for k in ["credentials", "fastq_temp"]):
        logger.warning("HCP not configured")
        return samples

    _futures: dict[Future, tuple[int, int, Path]] = {}
    with ThreadPoolExecutor(max_workers=config.hcp.parallel) as pool:
        for s_idx, sample in enumerate(samples):
            if all(Path(f).exists() for f in sample.files):
                logger.info(f"All files for {sample.id} found locally")
                continue
            elif sample.hcp_remote_keys is None:
                logger.warning(f"No backup for {sample.id}")
                sample.files = []
            else:
                sample.files = []
                for f_idx, remote_key in enumerate(sample.hcp_remote_keys):
                    logger.debug(f"Fetching {remote_key}")
                    _local_path = config.hcp.fastq_temp / Path(remote_key).name
                    _future = pool.submit(
                        _fetch,
                        credentials=config.hcp.credentials,
                        local_path=_local_path,
                        remote_key=remote_key,
                    )
                    _futures[_future] = (s_idx, f_idx, _local_path)

    if not _futures:
        return samples

    logger.info(f"Fetching {len(_futures)} files from HCP")

    _failed: list[int] = []
    for f in as_completed(_futures):
        s_idx, f_idx, local_path = _futures[f]
        try:
            location = f.result()
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"Failed to fetch {local_path.name} ({exc})")
            samples[s_idx].files = []
            samples[s_idx].hcp_remote_keys = None
            _failed.append(s_idx)
        else:
            if s_idx not in _failed:
                logger.info(f"Fetched {local_path.name} ({location})")
                samples[s_idx].files.insert(f_idx, local_path)

    return samples

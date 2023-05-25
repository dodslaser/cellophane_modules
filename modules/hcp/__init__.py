"""Module for fetching files from HCP."""

import sys
from concurrent.futures import ProcessPoolExecutor, Future, as_completed
from logging import LoggerAdapter
from pathlib import Path
from typing import Sequence
from attrs import define, field
from cellophane import cfg, data, modules
from NGPIris import hcp


@define(slots=False, init=False)
class HCPSample(data.Sample):
    """Sample with HCP backup."""

    backup: list[str] | None = field(default=None)

    @backup.validator
    def validate_backup(self, attribute: str, value: Sequence[str] | None) -> None:
        if not (
            value is None
            or (isinstance(value, Sequence) and all(isinstance(v, str) for v in value))
        ):
            raise ValueError(f"Invalid {attribute} value: {value}")


def _fetch(
    logdir: Path,
    credentials: Path,
    local_path: Path,
    remote_key: str,
    s_idx: int,
    f_idx: int,
) -> tuple[int, int, Path]:
    sys.stdout = open(
        logdir / f"iris.{local_path.name}.out", "w", encoding="utf-8"
    )
    sys.stderr = open(
        logdir / f"iris.{local_path.name}.err", "w", encoding="utf-8"
    )

    hcpm = hcp.HCPManager(
        credentials_path=credentials,
        bucket="data",  # FIXME: make this configurable
    )

    hcpm.download_file(
        remote_key,
        local_path=str(local_path),
        callback=False,
        force=True,
    )

    return s_idx, f_idx, local_path


@modules.pre_hook(label="HCP", after=["slims_fetch"])
def hcp_fetch(
    samples: data.Samples,
    config: cfg.Config,
    logger: LoggerAdapter,
    **_,
) -> data.Samples:
    """Fetch files from HCP."""

    _futures: list[Future] = []
    with ProcessPoolExecutor(max_workers=config.iris.parallel) as pool:
        for s_idx, sample in enumerate(samples):
            for f_idx, remote_key in enumerate(sample.backup):
                if sample.files[f_idx].exists():
                    logger.info(f"Found {sample.files[f_idx].name} locally")
                    continue

                local_path = config.iris.fastq_temp / Path(remote_key).name
                if local_path.exists():
                    logger.info(f"Found {local_path.name} locally")
                    continue

                else:
                    logger.info(f"Fetching {remote_key} from HCP")
                    _future = pool.submit(
                        _fetch,
                        logdir=config.logdir,
                        credentials=config.iris.credentials,
                        local_path=local_path,
                        remote_key=remote_key,
                        s_idx=s_idx,
                        f_idx=f_idx,
                    )
                    _futures.append(_future)

    _failed: list[int] = []
    for f in as_completed(_futures):
        try:
            s_idx, f_idx, local_path = f.result()
        except Exception as e:
            logger.error(f"Failed to fetch {local_path.name} from HCP ({e})")
            samples[s_idx].files = []
            _failed.append(s_idx)
        else:
            if s_idx not in _failed:
                logger.info(f"Fetched {local_path.name} from HCP")
                samples[s_idx].files.insert(f_idx, local_path)

    return samples

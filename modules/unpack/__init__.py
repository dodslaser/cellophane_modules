"""Module for fetching files from HCP."""

import multiprocessing as mp
from functools import partial
from logging import LoggerAdapter
from pathlib import Path
from time import sleep

from cellophane import Config, Executor, Sample, Samples, pre_hook
from mpire.async_result import AsyncResult

from .src.extractors import Extractor, PetageneExtractor, SpringExtractor

extractors: dict[str, Extractor] = {
    ".fasterq": PetageneExtractor(),
    ".spring": SpringExtractor(),
}


def _callback(
    *args,
    extractor: Extractor,
    timeout: int,
    sample: Sample,
    idx: int,
    logger: LoggerAdapter,
    path: Path,
) -> None:
    del args  # Unused

    logger.debug(f"Waiting up to {timeout} seconds for files to become available")
    while not (extracted_paths := [*extractor.extracted_paths(path)]) and timeout:
        sleep(1)
        timeout -= 1

    if not extracted_paths:
        logger.error(f"Failed to extract {path.name}")

    for extracted_path in extracted_paths:
        logger.debug(f"Extracted {extracted_path.name}")
        sample.files.insert(idx, extracted_path)

    if path in sample.files:
        sample.files.remove(path)


def _error_callback(
    *args,
    sample: Sample,
    logger: LoggerAdapter,
    path: Path,
) -> None:
    del args  # Unused
    logger.error(f"Failed to extract {path.name}")
    if path in sample.files:
        sample.files.remove(path)


@pre_hook(label="unpack", after=["hcp_fetch"])
def unpack(
    samples: Samples,
    config: Config,
    logger: LoggerAdapter,
    executor: Executor,
    **_,
) -> Samples:
    """Extract petagene fasterq files."""
    results: list[AsyncResult] = []
    for sample, idx, path, extractor in (
        (s, i, p, extractors[Path(p).suffix])
        for s in samples
        for i, p in enumerate(s.files)
        if Path(p).suffix in extractors
    ):
        if result := extractor.extract(
            logger=logger,
            compressed_path=path,
            config=config,
            executor=executor,
            callback=partial(
                _callback,
                extractor=extractor,
                timeout=config.unpack.timeout,
                sample=sample,
                idx=idx,
                logger=logger,
                path=path,
            ),
            error_callback=partial(
                _error_callback,
                sample=sample,
                logger=logger,
                path=path,
            ),
        ):
            results.append(result)

    executor.wait()
    return samples

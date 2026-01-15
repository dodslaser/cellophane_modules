"""Module for fetching files from HCP."""

from copy import copy
from logging import LoggerAdapter
from pathlib import Path
from threading import Lock
from time import sleep

from cellophane import Cleaner, Sample

from .extractors import Extractor


def callback(
    result: None,
    /,
    extractor: Extractor,
    timeout: int,
    sample: Sample,
    logger: LoggerAdapter,
    path: Path,
    cleaner: Cleaner,
    workdir: Path,
    sample_lock: Lock,
) -> None:
    del result  # Unused

    if not [*extractor.extracted_paths(workdir, path)]:
        logger.debug(f"Waiting up to {timeout} seconds for files to become available")
    _timeout = copy(timeout)
    while (
        not (extracted_paths := [*extractor.extracted_paths(workdir, path)])
        and (_timeout := _timeout - 1) > 0
    ):
        sleep(1)

    if not extracted_paths:
        logger.error(
            f"Extracted files for {path.name} "
            f"not found after {timeout} seconds"
        )

    with sample_lock:
        try:
            _idx = sample.files.index(path)
        except ValueError:
            logger.error(f"Compressed file '{path.name}' no longer in sample files")
            return
        else:
            sample.files.remove(path)
        for extracted_path in extracted_paths[::-1]:
            logger.debug(f"Extracted {extracted_path.name}")
            sample.files.insert(_idx, extracted_path)
            cleaner.register(extracted_path.resolve())



def error_callback(
    exception: Exception,
    /,
    sample: Sample,
    logger: LoggerAdapter,
    path: Path,
    extractor: Extractor,
    cleaner: Cleaner,
    workdir: Path,
) -> None:
    logger.error(f"Failed to extract {path.name}: {exception!r}")
    sample.fail(f"Failed to extract {path.name}")
    try:
        sample.files.remove(path)
    except ValueError:
        logger.error(f"Compressed file '{path.name}' no longer in sample files")

    for extracted_path in extractor.extracted_paths(workdir, path):
        cleaner.register(extracted_path.resolve())

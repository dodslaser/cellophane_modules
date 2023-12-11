"""Module for fetching files from HCP."""

import multiprocessing as mp
from logging import LoggerAdapter
from pathlib import Path
from time import sleep

from cellophane import cfg, data, modules

from .src.extractors import PetageneExtractor, SpringExtractor

extractors = {
    (".fasterq"): PetageneExtractor(),
    (".spring"): SpringExtractor(),
}


@modules.pre_hook(label="unpack", after=["hcp_fetch"])
def unpack(
    samples: data.Samples,
    config: cfg.Config,
    logger: LoggerAdapter,
    **_,
) -> data.Samples:
    """Extract petagene fasterq files."""
    _procs: list[mp.Process] = []
    _output_queue: mp.Queue = mp.Queue()

    for s_idx, sample in enumerate(samples):
        for f_idx, fastq in enumerate(sample.files):
            if fastq and (compressed_path := Path(fastq)).exists():
                for ext, extractor in extractors.items():
                    # FIXME: This will break for multi-extensions (e.g. .my.fancy.ext)
                    if compressed_path.suffix == ext:
                        samples[s_idx].files[f_idx] = None
                        _proc, _ = extractor.extract(
                            s_idx,
                            f_idx,
                            logger=logger,
                            compressed_path=compressed_path,
                            output_queue=_output_queue,
                            config=config,
                        )
                        if _proc:
                            _procs.append(_proc)

    while any(p.is_alive() for p in _procs) or not _output_queue.empty():
        s_idx, f_idx, extracted_path = _output_queue.get()
        samples[s_idx].files[f_idx] = extracted_path

    return samples

"""Module for fetching files from HCP."""

import multiprocessing as mp
from logging import LoggerAdapter
from pathlib import Path
from time import sleep
from cellophane import cfg, data, modules, sge
from functools import partial
from typing import Iterator, Callable


class Extractor:
    """Base class for extractors."""

    label: str
    script: Path

    def __init_subclass__(cls, label: str, script: Path) -> None:
        cls.label = label
        cls.script = script

    @staticmethod
    def extracted_paths(compressed_path: Path) -> Iterator[Path]:
        raise NotImplementedError

    @staticmethod
    def callback(
        *args,
        compressed_path: Path,
        extracted_paths_fn: Callable,
        output_queue: mp.Queue,
        logger: LoggerAdapter,
    ) -> None:
        if extracted_paths := [*extracted_paths_fn(compressed_path)]:
            for p in extracted_paths:
                logger.debug(f"Extracted {p.name}")
                output_queue.put((*args, p))
        else:
            logger.error(f"Failed to extract {compressed_path.name}")

    @staticmethod
    def error_callback(
        code: int,
        compressed_path: Path,
        logger: LoggerAdapter,
    ) -> None:
        logger.error(f"Failed to extract {compressed_path.name} ({code}))")

    def extract(
        self,
        *args,
        logger: LoggerAdapter,
        compressed_path: Path,
        output_queue: mp.Queue,
        config: cfg.Config,
        env: dict = {},
    ) -> mp.Process | None:
        if [*self.extracted_paths(compressed_path)]:
            self.callback(
                *args,
                compressed_path=compressed_path,
                extracted_paths_fn=self.extracted_paths,
                output_queue=output_queue,
                logger=logger,
            )
            return None
        else:
            logger.info(f"Extracting {compressed_path.name} with {self.label}")
            return sge.submit(
                self.script,
                env={
                    **env,
                    "COMPRESSED_PATH": compressed_path,
                    "THREADS": config.unpack.sge_slots,
                },
                queue=config.unpack.sge_queue,
                pe=config.unpack.sge_pe,
                slots=config.unpack.sge_slots,
                name=f"unpack_{compressed_path.name}",
                stderr=config.logdir / f"{compressed_path.name}.{self.label}.err",
                stdout=config.logdir / f"{compressed_path.name}.{self.label}.out",
                cwd=compressed_path.parent,
                check=False,
                callback=partial(
                    self.callback,
                    *args,
                    compressed_path=compressed_path,
                    extracted_paths_fn=self.extracted_paths,
                    output_queue=output_queue,
                    logger=logger,
                ),
                error_callback=partial(
                    self.error_callback,
                    compressed_path=compressed_path,
                    logger=logger,
                ),
            )


class PetageneExtractor(
    Extractor,
    label="petagene",
    script=Path(__file__).parent / "scripts" / "petagene.sh",
):
    @staticmethod
    def extracted_paths(compressed_path: Path) -> Iterator[Path]:
        _base = compressed_path.name.partition(".")[0]
        _parent = compressed_path.parent
        if (extracted := _parent / f"{_base}.fastq.gz").exists():
            yield extracted


class SpringExtractor(
    Extractor,
    label="spring",
    script=Path(__file__).parent / "scripts" / "spring.sh",
):
    @staticmethod
    def extracted_paths(compressed_path: Path) -> Iterator[Path]:
        _base = compressed_path.name.partition(".")[0]
        _parent = compressed_path.parent
        if (extracted := _parent / f"{_base}.fastq.gz").exists():
            yield extracted

        elif all(
            (
                (extracted1 := _parent / f"{_base}.1.fastq.gz").exists(),
                (extracted2 := _parent / f"{_base}.2.fastq.gz").exists(),
            )
        ):
            yield extracted1
            yield extracted2

        elif all(
            (
                (extracted1 := _parent / f"{_base}.fastq.gz.1").exists(),
                (extracted2 := _parent / f"{_base}.fastq.gz.2").exists(),
            )
        ):
            yield extracted1.rename(_parent / f"{_base}.1.fastq.gz")
            yield extracted2.rename(_parent / f"{_base}.2.fastq.gz")


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
                        _proc = extractor.extract(
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
        # This avoids locking when the queue empties before the processes finish
        # FIXME: Is there a better way to do this?
        sleep(1)

    return samples

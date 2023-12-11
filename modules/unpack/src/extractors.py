import multiprocessing as mp
from functools import partial
from logging import LoggerAdapter
from pathlib import Path
from typing import Callable, Iterator
from uuid import UUID

from cellophane import cfg, executors


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
        exception: Exception,
        compressed_path: Path,
        logger: LoggerAdapter,
    ) -> None:
        logger.error(f"Failed to extract {compressed_path.name} - {exception!r}")

    def extract(
        self,
        *args,
        logger: LoggerAdapter,
        compressed_path: Path,
        output_queue: mp.Queue,
        config: cfg.Config,
        env: dict = {},
        executor: executors.Executor,
    ) -> tuple[UUID, mp.Process] | None:
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
            return executor.submit(
                self.script,
                name=f"unpack_{compressed_path.name}",
                env={
                    **env,
                    "UNPACK_INIT": config.unpack.get("init", ""),
                    "UNPACK_EXIT": config.unpack.get("exit", ""),
                    "COMPRESSED_PATH": compressed_path,
                    "THREADS": config.unpack.threads,
                },
                cpus=config.unpack.threads,
                workdir=compressed_path.parent,
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
    script=Path(__file__).parent.parent / "scripts" / "petagene.sh",
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
    script=Path(__file__).parent.parent / "scripts" / "spring.sh",
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

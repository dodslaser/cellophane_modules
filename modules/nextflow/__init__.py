"""Module for fetching files from HCP."""

import multiprocessing as mp
from pathlib import Path
from typing import Mapping
from uuid import UUID, uuid4

from cellophane import cfg, data, executors


class NextflowSamples(data.Samples):
    def nfcore_samplesheet(self, *_, location: str | Path, **kwargs) -> Path:
        """Write a Nextflow samplesheet"""
        Path(location).mkdir(parents=True, exist_ok=True)
        _data = [
            {
                "sample": sample.id,
                "fastq_1": str(sample.files[0]),
                "fastq_2": str(sample.files[1]) if len(sample.files) > 1 else "",
                **{
                    k: v[sample.id] if isinstance(v, Mapping) else v
                    for k, v in kwargs.items()
                },
            }
            for sample in self
        ]

        _header = ",".join(_data[0].keys())

        _samplesheet = "\n".join([_header, *(",".join(d.values()) for d in _data)])
        _path = Path(location) / "samples.nextflow.csv"
        with open(_path, "w", encoding="utf-8") as handle:
            handle.write(_samplesheet)

        return _path


def nextflow(
    main: Path,
    *args,
    config: cfg.Config,
    env: dict[str, str] = {},
    workdir: Path | None = None,
    nf_config: Path | None = None,
    ansi_log: bool = False,
    resume: bool = False,
    name: str = "nextflow",
    executor: executors.Executor,
    **kwargs,
) -> tuple[mp.Process, UUID]:
    """Submit a Nextflow job to SGE."""
    (config.logdir / "nextflow").mkdir(exist_ok=True)

    if "workdir" in config.nextflow:
        config.nextflow.workdir.mkdir(parents=True, exist_ok=True)

    uuid = uuid4()

    return executor.submit(
        str(Path(__file__).parent / "scripts" / "nextflow.sh"),
        f"-log {config.logdir / 'nextflow' / f'{name}.{uuid.hex}.log'}",
        (
            f"-config {nf_config}"
            if nf_config
            else f"-config {c}"
            if (c := config.nextflow.get("config", None))
            else ""
        ),
        f"run {main}",
        "-ansi-log false" if not ansi_log or config.nextflow.ansi_log else "",
        f"-work-dir {workdir}" if workdir else "",
        "-resume" if resume else "",
        f"-with-report {config.logdir / 'nextflow' / f'{name}.{uuid.hex}.report.html'}",
        (
            f"-profile {p}"
            if (p := config.nextflow.get("profile", None))
            else ""
        ),
        *args,
        env={
            "_NXF_MODULE": config.nextflow.nf_module,
            "_JAVA_MODULE": config.nextflow.java_module,
            **{k: v for m in config.nextflow.env for k, v in m.items()},
            **env,
        },
        uuid=uuid,
        name=name,
        cpus=config.nextflow.threads,
        **kwargs,
    )

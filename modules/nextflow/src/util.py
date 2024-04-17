"""Module for fetching files from HCP."""

from pathlib import Path
from uuid import UUID, uuid4

from cellophane import cfg, executors
from mpire.async_result import AsyncResult


def nextflow(
    main: Path,
    *args,
    config: cfg.Config,
    executor: executors.Executor,
    workdir: Path,
    env: dict[str, str] | None = None,
    nxf_config: Path | None = None,
    nxf_work: Path | None = None,
    nxf_profile: str | None = None,
    ansi_log: bool = False,
    resume: bool = False,
    name: str = "nextflow",
    check: bool = True,
    **kwargs,
) -> tuple[AsyncResult, UUID]:
    """Submit a Nextflow job to SGE."""

    uuid = uuid4()
    _nxf_log = config.logdir / "nextflow" / f"{name}.{uuid.hex}.log"
    _nxf_config = nxf_config or config.nextflow.get("config")
    _nxf_work = nxf_work or config.nextflow.get("workdir") or workdir / "nxf_work"
    _nxf_launch = workdir / "nxf_launch"
    _nxf_profile = nxf_profile or config.nextflow.get("profile")

    _nxf_log.parent.mkdir(parents=True, exist_ok=True)
    _nxf_work.mkdir(parents=True, exist_ok=True)

    result, uuid = executor.submit(
        str(Path(__file__).parent / "scripts" / "nextflow.sh"),
        f"-log {_nxf_log}",
        (f"-config {_nxf_config}" if _nxf_config else ""),
        f"run {main}",
        "-ansi-log false" if not ansi_log or config.nextflow.ansi_log else "",
        f"-work-dir {_nxf_work}",
        "-resume" if resume else "",
        f"-with-report {config.logdir / 'nextflow' / f'{name}.{uuid.hex}.report.html'}",
        (f"-profile {_nxf_profile}" if _nxf_profile else ""),
        *args,
        env={
            "_NXF_INIT": config.nextflow.init,
            **config.nextflow.env,
            **(env or {}),
        },
        workdir=_nxf_launch,
        uuid=uuid,
        name=name,
        cpus=config.nextflow.threads,
        **kwargs,
    )

    if check:
        result.get()

    return result, uuid

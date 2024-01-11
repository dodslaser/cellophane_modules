from copy import copy
from functools import partial
from logging import LoggerAdapter
from pathlib import Path

from cellophane import cfg, data, executors, modules
from humanfriendly import parse_size
from mpire.async_result import AsyncResult


def _sync_callback(
    result: AsyncResult,
    /,
    logger: LoggerAdapter,
    outputs: list[data.Output],
):
    del result  # Unused
    for o in outputs:
        if o.dst.exists():
            logger.debug(f"Copied {o.src} -> {o.dst}")
        else:
            logger.warning(f"{o.dst} is missing")


@modules.post_hook(label="Sync Output", condition="complete")
def rsync_results(
    samples: data.Samples,
    logger: LoggerAdapter,
    config: cfg.Config,
    workdir: Path,
    executor: executors.Executor,
    **_,
) -> None:
    if "rsync" not in config:
        logger.info("Rsync not configured")
        return
    elif not samples.output:
        logger.warning("No output to sync")
        return
    else:
        logger.info(f"Syncing output to {config.resultdir}")

    # Split outputs into large files, small files, and directories
    labels: dict[str, str] = {
        "large": f"large files (>{config.rsync.large_file_threshold})",
        "small": f"small files (<{config.rsync.large_file_threshold})",
        "dir": "directories",
    }
    manifests: dict[str, list[tuple[str, str]]] = {
        "large": [],
        "small": [],
        "dir": [],
    }

    for output in samples.output.copy():
        if not output.src.exists():
            logger.warning(f"{output.src} does not exist")
            samples.output.remove(output)
            continue
        elif output.dst.exists() and not config.rsync.overwrite:
            logger.warning(f"{output.dst} already exists")
            samples.output.remove(output)
            continue
        elif not output.dst.is_relative_to(config.resultdir):
            logger.warning(f"{output.dst} is outside {config.rsync.base}")
            samples.output.remove(output)
            continue

        output.dst.parent.mkdir(parents=True, exist_ok=True)
        if output.src.is_dir():
            manifests["dir"] += [
                (f"{output.src.absolute()}/", f"{output.dst.absolute()}")
            ]
        elif output.src.stat().st_size > parse_size(config.rsync.large_file_threshold):
            manifests["large"] += [
                (f"{output.src.absolute()}", f"{output.dst.absolute()}")
            ]
        else:
            manifests["small"] += [
                (f"{output.src.absolute()}", f"{output.dst.absolute()}")
            ]

    for type_, manifest in manifests.items():
        if manifest:
            logger.info(f"Syncing {len(manifest)} {labels[type_]}")
            manifest_path = workdir / f"rsync.{type_}.manifest"
            with open(manifest_path, "w", encoding="utf-8") as m:
                m.writelines([f"{src} {dst}\n" for src, dst in manifest])
            executor.submit(
                str(Path(__file__).parent / "scripts" / "rsync.sh"),
                name="rsync",
                env={"MANIFEST": str(manifest_path)},
                callback=partial(
                    _sync_callback,
                    logger=logger,
                    manifest=manifest,
                ),
            )

    executor.wait()
    logger.info("Finished syncing output")

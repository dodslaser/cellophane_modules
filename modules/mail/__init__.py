from copy import copy
from email.message import EmailMessage
from logging import LoggerAdapter
from mimetypes import guess_type
from pathlib import Path
from smtplib import SMTP
from typing import Literal, Sequence

from attrs import define, field
from cellophane import Config, Sample, Samples, post_hook, pre_hook
from jinja2 import Environment
from mistletoe import markdown


@define(slots=False)
class MailSample(Sample):
    """A sample with mail attachments"""

    mail_attachments: set[Path] = field(factory=set)


@define(slots=False)
class MailSamples(Samples[MailSample]):
    """A collection of samples with mail attachments"""

    _mail_attachments: set[Path] = field(factory=set, init=False)

    @property
    def mail_attachments(self) -> set[Path]:
        """Get the mail attachments of all samples and the collection itself"""
        return {a for s in self for a in s.mail_attachments} | self._mail_attachments

    @mail_attachments.setter
    def mail_attachments(self, value: Sequence[Path]) -> None:
        self._mail_attachments = {*value}


@Sample.merge.register("mail_attachments")
@Samples.merge.register("_mail_attachments")
def _(this, that):
    return this | that


def _send_mail(
    *,
    from_addr: str,
    to_addr: list[str] | str,
    subject: str,
    body: str,
    host: str,
    port: int,
    tls: bool = False,
    cc_addr: list[str] | str | None = None,
    user: str | None = None,
    password: str | None = None,
    attachments: set[Path] | None = None,
    **_,
) -> None:
    conn = SMTP(host, port)
    if tls:
        conn.starttls()
    if user and password:
        conn.login(user, password)
    msg = EmailMessage()
    msg.set_content(body, subtype="html")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addr) if isinstance(to_addr, list) else to_addr
    if cc_addr is not None:
        msg["Cc"] = ", ".join(cc_addr) if isinstance(cc_addr, list) else cc_addr

    for attachment in attachments or []:
        ctype, encoding = guess_type(attachment)
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        with open(attachment, "rb") as fp:
            msg.add_attachment(
                fp.read(),
                maintype=maintype,
                subtype=subtype,
                filename=Path(attachment).name,
            )

    conn.send_message(msg)
    conn.quit()


def _render_mail(subject, body, **kwargs):
    body_template = Environment().from_string(body)
    subject_template = Environment().from_string(subject)

    subject_ = subject_template.render(**kwargs)
    body_ = markdown(body_template.render(**kwargs))
    return subject_, body_


def _resolve_attachments(
    attachments: Sequence[str],
    logger: LoggerAdapter,
    samples: Samples,
    config: Config,
) -> set[Path]:
    attachments_ = set()
    for sample in samples:
        attachments_ |= {
            Path(
                a.format(
                    sample=sample,
                    samples=samples,
                    config=config,
                )
            )
            for a in attachments
        }

    for attachment in attachments_.copy():
        if attachment.is_symlink() or not attachment.is_absolute():
            attachment = attachment.resolve()

        if attachment.is_dir():
            logger.warning(f"Attachment {attachment} is a directory")
            attachments_.remove(attachment)

        elif not attachment.is_file():
            logger.warning(f"Attachment {attachment} is not a file")
            attachments_.remove(attachment)

    return attachments_


def _mail_hook(
    samples: Samples,
    logger: LoggerAdapter,
    config: Config,
    workdir: Path,
    when: Literal["start", "end"],
    **_,
):
    if not samples:
        logger.info(f"No samples to send {when} mail for")
        return samples
    if config.mail.send:
        logger.info(f"Sending {when} mail")
        subject, body = _render_mail(
            subject=config.mail[when].subject,
            body=config.mail[when].body,
            samples=samples,
            config=config,
        )

        attachments = _resolve_attachments(
            attachments=samples.mail_attachments | {*config.mail[when].attachments},
            logger=logger,
            samples=samples,
            config=config,
        )

        if when == "end":
            attachments |= _resolve_attachments(
                attachments=config.mail[when].attachments_complete,
                logger=logger,
                samples=samples.complete,
                config=config,
            )

            attachments |= _resolve_attachments(
                attachments=config.mail[when].attachments_failed,
                logger=logger,
                samples=samples.failed,
                config=config,
            )

        logger.debug(f"Subject: {subject}")
        logger.debug(f"From: {config.mail.from_addr}")
        for to in config.mail.to_addr:
            logger.debug(f"To: {to}")
        for cc in config.mail.get("cc_addr", []):
            logger.debug(f"Cc: {cc}")
        for a in attachments:
            logger.debug(f"Attachment: {a}")
        logger.debug(f"Body:\n{body}")

        _send_mail(
            **config.mail.smtp,
            body=body,
            subject=subject,
            to_addr=config.mail.to_addr,
            from_addr=config.mail.from_addr,
            cc_addr=config.mail.get("cc_addr"),
            attachments=attachments,
            attachment_parent=workdir,
        )

    return samples


@pre_hook(label="Send start mail", after="all")
def start_mail(
    samples: Samples,
    logger: LoggerAdapter,
    config: Config,
    workdir: Path,
    **_,
):
    """Send a mail at the start of the analysis"""
    return _mail_hook(
        samples=samples,
        logger=logger,
        config=config,
        workdir=workdir,
        when="start",
    )


@post_hook(label="Send end mail", condition="always", after="all")
def end_mail(
    samples: Samples,
    logger: LoggerAdapter,
    config: Config,
    workdir: Path,
    **_,
):
    """Send a mail at the end of the analysis"""
    return _mail_hook(
        samples=samples,
        logger=logger,
        config=config,
        workdir=workdir,
        when="end",
    )

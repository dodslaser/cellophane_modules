from cellophane import Config, error_hook


@error_hook(label="Sentry (Session)")
def sentry_session(exception: BaseException, config: Config, logger: Logger) -> None:
    logger.debug("Capturing session exception with Sentry")
    config.sentry_sdk.capture_exception(exception)

@error_hook(label="Sentry (Runner)")
def sentry_runner(exception: BaseException, config: Config, logger: Logger) -> None:
    logger.debug("Capturing runner exception with Sentry")
    config.sentry_sdk.capture_exception(exception)
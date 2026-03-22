from __future__ import annotations

import logging
import os
import sys


def configure_logging(service_name: str, *, instance_name: str) -> logging.Logger:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s service=%(name)s message=%(message)s",
        stream=sys.stdout,
        force=True,
    )

    return logging.getLogger(f"{service_name}.{instance_name}")

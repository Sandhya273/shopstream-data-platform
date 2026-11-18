"""Structured JSON logging (CloudWatch Logs Insights friendly) + a run-metrics helper."""
from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "ctx", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(JsonFormatter())
        logger.addHandler(h)
        logger.setLevel(level)
        logger.propagate = False
    return logger


def log(logger: logging.Logger, msg: str, level: int = logging.INFO, **ctx) -> None:
    logger.log(level, msg, extra={"ctx": ctx})


@contextmanager
def timed(logger: logging.Logger, step: str, **ctx):
    start = time.perf_counter()
    log(logger, f"{step} started", step=step, **ctx)
    try:
        yield
    except Exception:
        log(logger, f"{step} failed", logging.ERROR, step=step, seconds=round(time.perf_counter() - start, 2), **ctx)
        raise
    log(logger, f"{step} finished", step=step, seconds=round(time.perf_counter() - start, 2), **ctx)

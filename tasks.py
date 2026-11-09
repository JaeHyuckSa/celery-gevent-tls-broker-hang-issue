"""Minimal Celery app on a TLS (amqps://) broker."""

import os
import ssl
from typing import Any

import celery
from celery import bootsteps
from celery.signals import worker_ready

app: celery.Celery = celery.Celery("tasks", broker=os.environ.get("BROKER_URL", "amqps://guest:guest@localhost:5671//"))
app.conf.broker_use_ssl = {"cert_reqs": ssl.CERT_NONE}
app.conf.broker_heartbeat = 10  # default 120; shorter so the lost connection is noticed in ~20 s
app.conf.broker_connection_retry_on_startup = True


class KeepConnectionTimeout(bootsteps.Step):
    """Workaround: undo `broker_connection_timeout = None`, which Consumer.__init__ sets under gevent."""

    def __init__(self, parent: Any, **kwargs: Any) -> None:
        parent.app.conf.broker_connection_timeout = 4.0  # Celery's default


if os.environ.get("WORKAROUND") == "1":
    app.steps["consumer"].add(KeepConnectionTimeout)


@worker_ready.connect
def _report(sender: Any, **kwargs: Any) -> None:
    """Logs the connection timeout setting and the read timeout of the broker's TLS socket."""
    sock = sender.connection.connection.transport.sock
    print(f"broker_connection_timeout={sender.app.conf.broker_connection_timeout}, "
          f"TLS socket timeout={sock.gettimeout()}")

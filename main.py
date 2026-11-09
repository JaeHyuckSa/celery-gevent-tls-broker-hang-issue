"""Celery gevent worker on a TLS broker hangs without logging when the broker stops answering.

Usage:
    uv run python main.py [--workaround]
"""

import os
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import datetime

CONTAINER: str = "celery-gevent-tls-hang-rabbitmq"
IMAGE: str = "rabbitmq:3.13.7-management"
PORT: str = os.environ.get("BROKER_PORT", "5671")
FREEZE: float = 90.0
WORKER_LOG: str = os.path.join(tempfile.gettempdir(), "_celery_gevent_tls_hang_worker.log")
# Self-signed certificate and TLS listener, created inside the container.
BROKER_SETUP: str = (
    "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost"
    " -keyout /tmp/key.pem -out /tmp/cert.pem 2>/dev/null && chmod 644 /tmp/key.pem /tmp/cert.pem"
    " && printf 'listeners.ssl.default = 5671\\nssl_options.certfile = /tmp/cert.pem\\n"
    "ssl_options.keyfile = /tmp/key.pem\\n' > /etc/rabbitmq/conf.d/90-tls.conf"
    " && exec docker-entrypoint.sh rabbitmq-server"
)
LINE: re.Pattern[str] = re.compile(r"^\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}): (\w+)/\w+\] (.*)$")


def docker(*args: str) -> str:
    """Runs a docker CLI command and returns its output."""
    return subprocess.run(["docker", *args], capture_output=True, text=True).stdout


def wait_for(condition: Callable[[], bool], timeout: float, what: str) -> None:
    """Polls condition until it is true."""
    deadline: float = time.time() + timeout
    while not condition():
        if time.time() > deadline:
            raise RuntimeError(f"Timed out waiting for {what}, see {WORKER_LOG}")
        time.sleep(0.5)


def worker_lines() -> list[tuple[float, str, str]]:
    """Returns (epoch, level, message) for each Celery log line of the worker."""
    with open(WORKER_LOG) as f:
        matches = [LINE.match(line) for line in f]
    return [(datetime.strptime(m[1], "%Y-%m-%d %H:%M:%S,%f").timestamp(), m[2], m[3]) for m in matches if m]


def main() -> None:
    """Starts a TLS RabbitMQ and a gevent worker, freezes the broker, and measures the worker's logs."""
    workaround: bool = "--workaround" in sys.argv[1:]

    import amqp
    import celery
    import gevent
    import kombu

    print(f"Celery {celery.__version__}, kombu {kombu.__version__}, py-amqp {amqp.__version__}, "
          f"gevent {gevent.__version__}, Python {sys.version.split()[0]}")

    docker("rm", "-f", CONTAINER)
    docker("run", "-d", "--name", CONTAINER, "-p", f"{PORT}:5671", "--entrypoint", "sh", IMAGE, "-c", BROKER_SETUP)
    worker: subprocess.Popen[bytes] | None = None
    try:
        wait_for(lambda: "Server startup complete" in docker("logs", CONTAINER), 120, "RabbitMQ")
        with open(WORKER_LOG, "w") as log:
            worker = subprocess.Popen(
                [sys.executable, "-m", "celery", "-A", "tasks", "worker", "-P", "gevent", "--loglevel=INFO"],
                env={**os.environ, "BROKER_URL": f"amqps://guest:guest@localhost:{PORT}//",
                     "WORKAROUND": "1" if workaround else "0"},
                cwd=os.path.dirname(os.path.abspath(__file__)),
                stdout=log,
                stderr=log,
            )
        wait_for(lambda: any(" ready." in m for _, _, m in worker_lines()), 60, "the worker")
        print(next(m for _, _, m in worker_lines() if m.startswith("broker_connection_timeout=")))

        docker("pause", CONTAINER)  # TCP stays up, the broker never answers
        frozen: float = time.time()
        time.sleep(FREEZE)
        resumed: float = time.time()
        docker("unpause", CONTAINER)
        time.sleep(15)
    finally:
        if worker:
            worker.terminate()
            try:
                worker.wait(10)
            except subprocess.TimeoutExpired:
                worker.kill()
        docker("unpause", CONTAINER)
        docker("rm", "-f", CONTAINER)

    lines = worker_lines()
    during = [(t, level, m) for t, level, m in lines if frozen <= t < resumed]
    retries: int = sum("Cannot connect" in m for _, _, m in during)
    silent: float = resumed - max((t for t, _, _ in during), default=frozen)
    reconnect: float | None = next((t - resumed for t, _, m in lines if t >= resumed and "Connected to" in m), None)

    for t, level, m in during[:1]:
        print(f"+{t - frozen:.1f}s {level} {m.split(' (', 1)[0]}")
    print(f"Reconnect attempts logged during the {FREEZE:.0f}s freeze: {retries}")
    if reconnect is not None:
        print(f"Reconnected {reconnect:.1f}s after the broker resumed")
    verdict: str = "HANG CONFIRMED" if retries == 0 and silent >= 30 else "NOT REPRODUCED"
    print(f"Worker silent for the last {silent:.1f}s of the freeze  {verdict}")


if __name__ == "__main__":
    main()

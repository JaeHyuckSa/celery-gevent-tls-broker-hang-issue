# celery-gevent-tls-broker-hang-issue

Reproduction: a `celery worker -P gevent` on a TLS (`amqps://`) broker hangs without logging when the broker stops answering.

## Bug

Under gevent, `Consumer.__init__` sets `broker_connection_timeout = None` (`celery/worker/consumer/consumer.py:259`). This is documented, but it is a 2012 workaround for a gevent bug that no longer reproduces. On TLS it removes the only read deadline, because py-amqp keeps `connect_timeout` as the socket timeout (`amqp/transport.py:408`). When the heartbeat check restarts the consumer, `pidbox.on_stop` waits for the pidbox greenlet, which waits with no timeout for `Connection.CloseOk` (`amqp/connection.py:605`), so the worker logs one warning and nothing more until the broker answers or closes the connection (celery 5.7.0, py-amqp 5.4.2, gevent 26.9.0). On celery 5.3-5.6 it hangs earlier, in the TLS shutdown of the consumer connection.

```
broker_connection_timeout=None, TLS socket timeout=None
+25.2s WARNING consumer: Connection to broker lost. Trying to re-establish the connection...
Reconnect attempts logged during the 90s freeze: 0
Worker silent for the last 59.8s of the freeze  HANG CONFIRMED
```

## Reproduce

Requires Docker and Python 3.11+. The broker is frozen with `docker pause`: TCP stays up, but nothing answers, as behind a proxy or sidecar. A stopped broker is retried normally.

```bash
uv sync
uv run python main.py
```

With the workaround:

```bash
uv run python main.py --workaround
```

## Expected Result

```
Worker silent for the last 59.8s of the freeze  HANG CONFIRMED
```

## Workaround

Put the connection timeout back after the consumer clears it:

```python
class KeepConnectionTimeout(bootsteps.Step):
    def __init__(self, parent, **kwargs):
        parent.app.conf.broker_connection_timeout = 4.0

app.steps["consumer"].add(KeepConnectionTimeout)
```

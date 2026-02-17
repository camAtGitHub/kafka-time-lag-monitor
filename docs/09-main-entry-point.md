## Main Entry Point

### `main.py` Responsibilities

- Parse CLI arguments: `--config <path>` (required)
- Load and validate config
- Initialise database connection
- Initialise state manager (loads persisted statuses from DB)
- Build Kafka AdminClient
- Verify Kafka connectivity at startup — retry with backoff up to a configurable timeout before exiting with a clear error
- Instantiate sampler, reporter, and housekeeping objects
- Start all three as daemon threads
- Register `SIGTERM` and `SIGINT` handlers that set a shared `threading.Event` called `shutdown_event`
- Main thread loops on `shutdown_event.wait(timeout=5)`, logging a periodic heartbeat
- On shutdown: set event, join all threads with timeout, close DB connection, exit 0

### Thread Restart Wrapper

Each thread's `run()` loop is wrapped in an outer loop that catches unexpected exceptions, logs a full traceback, and restarts the thread after a 30-second backoff. Threads check `shutdown_event.is_set()` at the top of every inner cycle and exit cleanly when set. The restart wrapper also checks the shutdown event before restarting to avoid restarting threads during a clean shutdown.

### Error Handling Philosophy

| Failure | Behaviour |
|---|---|
| Config file missing or invalid | Hard exit at startup with clear error |
| Kafka unreachable at startup | Retry with backoff, exit after timeout |
| Kafka unreachable mid-run | Log warning, skip cycle, try again next cycle |
| Database error | Log error with traceback, attempt reconnect on next cycle |
| Interpolation returns nonsensical value | Log warning, emit lag_seconds=0 |
| Unhandled thread exception | Log full traceback, restart thread after 30s backoff |


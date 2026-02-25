"""Main entry point module.

Handles CLI arguments, thread lifecycle, signal handling, and clean shutdown.
"""

import argparse
import logging
import os
import signal
import sys
import threading
import time
from typing import Any, Optional

import config as config_module
import database
import housekeeping
import kafka_client
import reporter
import sampler
import state_manager


logger = logging.getLogger(__name__)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging for the application.

    Args:
        level: Logging level (default: INFO)
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def verify_kafka_connectivity(
    kafka_client_module: Any,
    config: Any,
    timeout_seconds: int = 120,
    retry_interval: int = 10,
) -> Any:
    """Verify Kafka connectivity at startup.

    Args:
        kafka_client_module: The kafka_client module
        config: Configuration object
        timeout_seconds: Maximum time to wait for connectivity
        retry_interval: Seconds between retries

    Returns:
        The verified AdminClient for reuse

    Raises:
        RuntimeError: If connection cannot be established within timeout
    """
    admin_client = kafka_client_module.build_admin_client(config)
    start_time = time.time()
    last_error = None

    while time.time() - start_time < timeout_seconds:
        try:
            groups = kafka_client_module.get_active_consumer_groups(admin_client)
            logger.info(
                f"Kafka connectivity verified: {len(groups)} consumer groups found"
            )
            return admin_client
        except Exception as e:
            last_error = e
            logger.warning(
                f"Kafka connectivity check failed: {e}. Retrying in {retry_interval}s..."
            )
            time.sleep(retry_interval)

    raise RuntimeError(
        f"Failed to connect to Kafka after {timeout_seconds}s: {last_error}"
    )


def run_with_restart(
    target_func: Any, shutdown_event: threading.Event, thread_name: str, *args: Any
) -> None:
    """Run a function with automatic restart on exception.

    Catches any unhandled exception, logs it, waits 30 seconds (checking
    shutdown_event during wait), then restarts the function.

    Args:
        target_func: The function to run
        shutdown_event: Event to signal shutdown
        thread_name: Name of the thread for logging
        *args: Arguments to pass to the function
    """
    while not shutdown_event.is_set():
        try:
            target_func(*args)
        except Exception:
            logger.exception(
                f"Unhandled exception in {thread_name}, waiting to restart..."
            )

            # Wait 30 seconds before restart, checking shutdown_event
            if shutdown_event.wait(timeout=30):
                break

            # Try again if not shutting down
            logger.info(f"Restarting {thread_name}...")


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 for clean shutdown)
    """
    # Parse CLI arguments
    parser = argparse.ArgumentParser(description="Kafka Consumer Lag Monitor")
    parser.add_argument(
        "--config", required=True, help="Path to configuration YAML file"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_logging(log_level)

    # Load configuration first
    try:
        cfg = config_module.load_config(args.config)
        logger.info(f"Configuration loaded from {args.config}")
    except config_module.ConfigError as e:
        print(f"ERROR: Invalid configuration: {e}", file=sys.stderr)
        return 1

    # Validate output directory exists
    output_dir = os.path.dirname(os.path.abspath(cfg.output.json_path)) or "."
    if not os.path.isdir(output_dir):
        logger.error(
            f"Output directory does not exist: {output_dir!r} "
            f"(from output.json_path: {cfg.output.json_path!r})"
        )
        return 1

    # Initialize database (creates tables, then we close this connection)
    try:
        init_conn = database.init_db(cfg.database.path)
        init_conn.close()
        logger.info(f"Database initialized at {cfg.database.path}")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return 1

    # Verify Kafka connectivity and get reusable client
    try:
        admin_client = verify_kafka_connectivity(kafka_client, cfg)
    except RuntimeError as e:
        logger.error(str(e))
        return 1

    # Create state manager (creates its own DB connection)
    state_mgr = state_manager.StateManager(cfg.database.path, cfg)

    # Create worker instances (each creates its own DB connection)
    sampler_instance = sampler.Sampler(cfg, cfg.database.path, admin_client, state_mgr)
    reporter_instance = reporter.Reporter(cfg, cfg.database.path, state_mgr)
    housekeeping_instance = housekeeping.Housekeeping(cfg, cfg.database.path)

    # Create shutdown event
    shutdown_event = threading.Event()

    # Setup signal handlers
    def handle_signal(signum: int, frame: Any) -> None:
        logger.info(f"Received signal {signum}, initiating shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Create and start threads with restart wrapper
    threads = []

    sampler_thread = threading.Thread(
        target=run_with_restart,
        args=(sampler_instance.run, shutdown_event, "sampler", shutdown_event),
        name="sampler",
        daemon=True,
    )
    threads.append(sampler_thread)

    reporter_thread = threading.Thread(
        target=run_with_restart,
        args=(reporter_instance.run, shutdown_event, "reporter", shutdown_event),
        name="reporter",
        daemon=True,
    )
    threads.append(reporter_thread)

    housekeeping_thread = threading.Thread(
        target=run_with_restart,
        args=(
            housekeeping_instance.run,
            shutdown_event,
            "housekeeping",
            shutdown_event,
        ),
        name="housekeeping",
        daemon=True,
    )
    threads.append(housekeeping_thread)

    # Start all threads
    for thread in threads:
        thread.start()
        logger.info(f"Started {thread.name} thread")

    # Main thread heartbeat
    try:
        while not shutdown_event.wait(timeout=30):
            # Log heartbeat at DEBUG level
            sampler_last = state_mgr.get_thread_last_run("sampler")
            reporter_last = state_mgr.get_thread_last_run("reporter")
            housekeeping_last = state_mgr.get_thread_last_run("housekeeping")

            logger.debug(
                f"Heartbeat: sampler={sampler_last}, "
                f"reporter={reporter_last}, housekeeping={housekeeping_last}"
            )
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        shutdown_event.set()

    # Shutdown
    logger.info("Shutting down threads...")

    deadline = time.time() + 10
    for thread in threads:
        remaining = max(0, deadline - time.time())
        thread.join(timeout=remaining)
        if thread.is_alive():
            logger.warning(f"Thread {thread.name} did not stop within timeout")

    logger.info("Shutdown complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())

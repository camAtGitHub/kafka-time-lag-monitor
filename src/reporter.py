"""Reporter thread module for JSON output generation.

Transforms database state into consumer-facing JSON output files.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Tuple, Optional

import database
import interpolation


logger = logging.getLogger(__name__)


class Reporter:
    """Reporter thread that generates JSON output from database state.
    
    Reads current state from the database, calculates lag for all monitored
    group/topic combinations, assembles a JSON blob, and writes it atomically
    to the configured output path.
    """
    
    def __init__(self, config: Any, db_conn: Any, state_manager: Any) -> None:
        """Initialize the reporter.
        
        Args:
            config: Configuration object with monitoring and output settings
            db_conn: SQLite database connection
            state_manager: StateManager instance for accessing group statuses
        """
        self._config = config
        self._db_conn = db_conn
        self._state_manager = state_manager
    
    def run(self, shutdown_event: Any) -> None:
        """Run the reporter loop.
        
        Continuously generates JSON output at the configured interval until
        shutdown_event is set.
        
        Args:
            shutdown_event: Threading event to signal shutdown
        """
        while not shutdown_event.is_set():
            try:
                cycle_start = time.time()
                self._run_cycle()
                self._state_manager.update_thread_last_run("reporter", int(time.time()))
                
                # Sleep for remainder of interval
                elapsed = time.time() - cycle_start
                sleep_time = max(0, self._config.monitoring.report_interval_seconds - elapsed)
                
                # Check shutdown_event during sleep
                if shutdown_event.wait(timeout=sleep_time):
                    break
                    
            except Exception as e:
                logger.exception(f"Error in reporter cycle: {e}")
                # Sleep briefly before retrying
                if shutdown_event.wait(timeout=5):
                    break
    
    def _run_cycle(self) -> None:
        """Execute a single reporter cycle."""
        now = int(time.time())
        consumers = []
        
        # Get all distinct (group_id, topic, partition) from consumer_commits
        commit_keys = database.get_all_commit_keys(self._db_conn)
        
        # Group by (group_id, topic)
        grouped: Dict[Tuple[str, str], List[int]] = {}
        for group_id, topic, partition in commit_keys:
            key = (group_id, topic)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(partition)
        
        # Process each (group_id, topic) combination
        for (group_id, topic), partitions in grouped.items():
            try:
                consumer_record = self._process_consumer(
                    group_id, topic, partitions, now
                )
                if consumer_record:
                    consumers.append(consumer_record)
            except Exception as e:
                logger.exception(
                    f"Error calculating lag for group={group_id}, topic={topic}: {e}"
                )
                # Skip this group/topic and continue with others
                continue
        
        # Assemble output
        output = {
            "generated_at": now,
            "consumers": consumers
        }
        
        # Write output atomically
        self._write_output(output)
        
        # Update state manager
        self._state_manager.set_last_json_output(output)
        
        logger.info(f"Reporter cycle complete: {len(consumers)} consumers written")
    
    def _process_consumer(
        self,
        group_id: str,
        topic: str,
        partitions: List[int],
        now: int
    ) -> Optional[Dict[str, Any]]:
        """Process a single consumer group/topic combination.
        
        Args:
            group_id: Consumer group ID
            topic: Topic name
            partitions: List of partition numbers
            now: Current timestamp
            
        Returns:
            Consumer record dict or None if processing fails
        """
        # Get status from state_manager
        status_dict = self._state_manager.get_group_status(group_id, topic)
        status = status_dict.get("status", "ONLINE").lower()
        
        # Calculate lag for each partition
        partition_lags: List[Tuple[int, int, str]] = []
        
        for partition in partitions:
            # Get most recent committed offset
            recent_commits = database.get_recent_commits(
                self._db_conn, group_id, topic, partition, limit=1
            )
            
            if not recent_commits:
                continue
            
            committed_offset = recent_commits[0][0]
            
            # Get interpolation points
            interpolation_points = database.get_interpolation_points(
                self._db_conn, topic, partition
            )
            
            # Calculate lag
            lag_seconds, method = interpolation.calculate_lag_seconds(
                committed_offset, interpolation_points, now
            )
            
            partition_lags.append((partition, lag_seconds, method))
        
        if not partition_lags:
            return None
        
        # Aggregate across partitions
        max_lag, worst_partition, _ = interpolation.aggregate_partition_lags(
            partition_lags
        )
        
        # Determine data_resolution
        data_resolution = "fine" if status == "online" else "coarse"
        
        # Build record
        record = {
            "group_id": group_id,
            "topic": topic,
            "lag_seconds": max_lag,
            "lag_display": interpolation.format_lag_display(max_lag),
            "worst_partition": worst_partition,
            "status": status,
            "data_resolution": data_resolution,
            "partitions_monitored": len(partitions),
            "calculated_at": now
        }
        
        return record
    
    def _write_output(self, output: Dict[str, Any]) -> None:
        """Write output JSON atomically.
        
        Writes to a temp file first, then uses os.replace() for atomic rename.
        
        Args:
            output: The output dictionary to write
        """
        json_path = self._config.output.json_path
        tmp_path = f"{json_path}.tmp"
        
        try:
            # Write to temp file
            with open(tmp_path, 'w') as f:
                json.dump(output, f, indent=2)
            
            # Atomic replace
            os.replace(tmp_path, json_path)
            
        except Exception:
            # Clean up temp file on error
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            raise

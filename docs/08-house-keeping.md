## Housekeeping Thread

### `housekeeping.py` Responsibilities

Runs every N minutes. Deliberately simple — knows nothing about group status, write cadence, or data tiers. Enforces count-based row limits and runs incremental vacuum. The simplicity is intentional: the sampler's write throttling design means coarser data naturally accumulates fewer rows, so a uniform count-based limit is sufficient without any special-casing.

### Cycle Logic

```
1. Get all distinct (topic, partition) pairs from partition_offsets
2. For each: call database.prune_partition_offsets(topic, partition, keep_n=config.max_entries_per_partition)
3. Get all distinct (group_id, topic, partition) triples from consumer_commits
4. For each: call database.prune_consumer_commits(group_id, topic, partition, keep_n=config.max_commit_entries_per_partition)
5. Call database.run_incremental_vacuum(pages=100)
6. Log summary: total rows pruned from each table, elapsed time
7. Sleep for housekeeping_interval_seconds
```

There is no quiet time assumption. `PRAGMA auto_vacuum=INCREMENTAL` combined with small incremental vacuum calls spreads reclamation work across cycles without ever blocking other threads for a significant duration.


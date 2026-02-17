"""Interpolation engine module for lag calculations.

Pure calculation functions with no I/O or side effects.
"""

from typing import List, Tuple, Optional


def interpolate_timestamp(
    committed_offset: int,
    lower_point: Tuple[int, int],
    upper_point: Tuple[int, int]
) -> int:
    """Interpolate timestamp for a given offset between two known points.

    Uses linear interpolation formula:
    (committed - lower_offset) / (upper_offset - lower_offset)
        = (x - lower_ts) / (upper_ts - lower_ts)

    Args:
        committed_offset: The offset to interpolate for
        lower_point: Tuple of (offset, timestamp) for the lower bound
        upper_point: Tuple of (offset, timestamp) for the upper bound

    Returns:
        Interpolated timestamp as integer

    Example:
        >>> interpolate_timestamp(250, (240, 1597304361), (260, 1597304421))
        1597304391
    """
    lower_offset, lower_ts = lower_point
    upper_offset, upper_ts = upper_point

    # Handle division by zero case
    if upper_offset == lower_offset:
        return upper_ts

    # Calculate interpolation
    offset_range = upper_offset - lower_offset
    ts_range = upper_ts - lower_ts
    offset_ratio = (committed_offset - lower_offset) / offset_range
    interpolated_ts = lower_ts + int(offset_ratio * ts_range)

    return interpolated_ts


def calculate_lag_seconds(
    committed_offset: int,
    interpolation_points: List[Tuple[int, int]],
    current_time: int
) -> Tuple[int, str]:
    """Calculate lag in seconds for a committed offset.

    Args:
        committed_offset: The consumer's committed offset
        interpolation_points: List of (offset, timestamp) tuples, sorted by
                             timestamp descending (newest first)
        current_time: Current timestamp in seconds

    Returns:
        Tuple of (lag_seconds, method) where method is one of:
        "no_data", "current", "exact", "interpolated", "extrapolated"
    """
    # Case 1: No data
    if not interpolation_points:
        return (0, "no_data")

    # Extract offsets and timestamps
    newest_offset, newest_ts = interpolation_points[0]
    oldest_offset, oldest_ts = interpolation_points[-1]

    # Case 2: Consumer is at or ahead of newest sample
    if committed_offset >= newest_offset:
        return (0, "current")

    # Case 3: Look for exact match
    # Use a dict to track latest timestamp for each offset (handles duplicates)
    offset_to_latest_ts = {}
    for offset, ts in interpolation_points:
        if offset not in offset_to_latest_ts or ts > offset_to_latest_ts[offset]:
            offset_to_latest_ts[offset] = ts

    if committed_offset in offset_to_latest_ts:
        lag = current_time - offset_to_latest_ts[committed_offset]
        return (lag, "exact")

    # Case 4: Find bracketing points for interpolation
    # Points are sorted by timestamp descending, so we need to find where
    # committed_offset falls between two points
    sorted_points = sorted(interpolation_points, key=lambda x: x[0])  # Sort by offset

    for i in range(len(sorted_points) - 1):
        lower_offset, lower_ts = sorted_points[i]
        upper_offset, upper_ts = sorted_points[i + 1]

        if lower_offset <= committed_offset <= upper_offset:
            interpolated_ts = interpolate_timestamp(
                committed_offset,
                (lower_offset, lower_ts),
                (upper_offset, upper_ts)
            )
            lag = current_time - interpolated_ts
            return (lag, "interpolated")

    # Case 5: Committed offset is below all table entries (extrapolation)
    lag = current_time - oldest_ts
    return (lag, "extrapolated")


def aggregate_partition_lags(
    partition_lag_list: List[Tuple[int, int, str]]
) -> Tuple[int, Optional[int], str]:
    """Aggregate lag information across multiple partitions.

    Args:
        partition_lag_list: List of (partition, lag_seconds, method) tuples

    Returns:
        Tuple of (max_lag_seconds, worst_partition, method_of_worst)
        If all lags are 0, worst_partition is None and method is "current"
    """
    if not partition_lag_list:
        return (0, None, "current")

    # Find max lag
    max_lag = max(lag for _, lag, _ in partition_lag_list)

    # If all lags are 0
    if max_lag == 0:
        return (0, None, "current")

    # Find the worst partition (first one with max lag)
    worst_partition = None
    worst_method = "current"

    for partition, lag, method in partition_lag_list:
        if lag == max_lag:
            worst_partition = partition
            worst_method = method
            break

    return (max_lag, worst_partition, worst_method)


def format_lag_display(lag_seconds: int) -> str:
    """Format lag seconds as human-readable string.

    Args:
        lag_seconds: Lag in seconds

    Returns:
        Human-readable string like "< 1 minute", "2 minutes", "1 hour 30 minutes"
    """
    if lag_seconds < 60:
        return "< 1 minute"

    # Calculate time components
    days = lag_seconds // 86400
    remaining = lag_seconds % 86400
    hours = remaining // 3600
    remaining = remaining % 3600
    minutes = remaining // 60

    parts = []

    # Add days
    if days > 0:
        parts.append(f"{days} day" if days == 1 else f"{days} days")

    # Add hours
    if hours > 0:
        parts.append(f"{hours} hour" if hours == 1 else f"{hours} hours")

    # Add minutes
    if minutes > 0:
        parts.append(f"{minutes} minute" if minutes == 1 else f"{minutes} minutes")

    return " ".join(parts)
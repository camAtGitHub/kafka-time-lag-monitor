"""Tests for interpolation.py module."""

import pytest
from interpolation import (
    interpolate_timestamp,
    calculate_lag_seconds,
    aggregate_partition_lags,
    format_lag_display,
)


class TestCalculateLagSeconds:
    """Tests for calculate_lag_seconds function."""

    def test_empty_interpolation_points_returns_no_data(self):
        """Empty interpolation_points → returns (0, "no_data")"""
        result = calculate_lag_seconds(100, [], 1597305000)
        assert result == (0, "no_data")

    def test_committed_offset_equals_newest_returns_current(self):
        """Committed offset equals newest offset in table → returns (0, "current")"""
        interpolation_points = [
            (300, 1597304421),
            (200, 1597304361),
        ]
        result = calculate_lag_seconds(300, interpolation_points, 1597305000)
        assert result == (0, "current")

    def test_committed_offset_greater_than_newest_returns_current(self):
        """Committed offset greater than newest offset → returns (0, "current")"""
        interpolation_points = [
            (300, 1597304421),
            (200, 1597304361),
        ]
        result = calculate_lag_seconds(350, interpolation_points, 1597305000)
        assert result == (0, "current")

    def test_exact_match_single_entry_returns_correct_lag(self):
        """Exact match, single entry → returns correct lag (when committed < newest)"""
        interpolation_points = [
            (300, 1597304421),  # newest
            (250, 1597304391),  # exact match target
        ]
        current_time = 1597305000
        result = calculate_lag_seconds(250, interpolation_points, current_time)
        lag = current_time - 1597304391
        assert result == (lag, "exact")

    def test_exact_match_duplicate_offsets_uses_latest_timestamp(self):
        """Exact match, duplicate offsets at different timestamps → uses latest timestamp (larger lag)"""
        interpolation_points = [
            (300, 1597304421),
            (250, 1597304491),  # Newer timestamp - should be used
            (250, 1597304391),  # Older timestamp
        ]
        current_time = 1597305000
        result = calculate_lag_seconds(250, interpolation_points, current_time)
        lag = current_time - 1597304491
        assert result == (lag, "exact")

    def test_interpolated_case_worked_example(self):
        """Interpolated case: committed=250, lower=(240, 1597304361), upper=(260, 1597304421)"""
        interpolation_points = [
            (260, 1597304421),
            (240, 1597304361),
        ]
        current_time = 1597305000
        result = calculate_lag_seconds(250, interpolation_points, current_time)
        expected_ts = 1597304391
        expected_lag = current_time - expected_ts
        assert result == (expected_lag, "interpolated")

    def test_extrapolated_case_below_all_entries(self):
        """Extrapolated case (committed_offset below all table entries)"""
        interpolation_points = [
            (300, 1597304421),
            (200, 1597304361),
        ]
        current_time = 1597305000
        result = calculate_lag_seconds(100, interpolation_points, current_time)
        oldest_ts = 1597304361
        expected_lag = current_time - oldest_ts
        assert result == (expected_lag, "extrapolated")


class TestInterpolateTimestamp:
    """Tests for interpolate_timestamp function."""

    def test_division_by_zero_returns_later_timestamp(self):
        """Division by zero (identical offsets) → returns later timestamp"""
        result = interpolate_timestamp(250, (240, 1597304361), (240, 1597304421))
        assert result == 1597304421

    def test_normal_interpolation(self):
        """Normal case: interpolate between two points"""
        result = interpolate_timestamp(250, (240, 1597304361), (260, 1597304421))
        assert result == 1597304391


class TestAggregatePartitionLags:
    """Tests for aggregate_partition_lags function."""

    def test_returns_max_lag_and_worst_partition(self):
        """Returns max lag and correct worst partition"""
        partition_lag_list = [
            (0, 100, "interpolated"),
            (1, 200, "interpolated"),
            (2, 50, "exact"),
        ]
        result = aggregate_partition_lags(partition_lag_list)
        assert result == (200, 1, "interpolated")

    def test_all_zeros_returns_none_and_current(self):
        """All zeros → returns (0, None, "current")"""
        partition_lag_list = [
            (0, 0, "current"),
            (1, 0, "current"),
        ]
        result = aggregate_partition_lags(partition_lag_list)
        assert result == (0, None, "current")

    def test_empty_list_returns_defaults(self):
        """Empty list → returns defaults"""
        result = aggregate_partition_lags([])
        assert result == (0, None, "current")


class TestFormatLagDisplay:
    """Tests for format_lag_display function."""

    def test_zero_returns_less_than_1_minute(self):
        """0 → "< 1 minute" """
        result = format_lag_display(0)
        assert result == "< 1 minute"

    def test_59_returns_less_than_1_minute(self):
        """59 → "< 1 minute" """
        result = format_lag_display(59)
        assert result == "< 1 minute"

    def test_60_returns_1_minute(self):
        """60 → "1 minute" """
        result = format_lag_display(60)
        assert result == "1 minute"

    def test_120_returns_2_minutes(self):
        """120 → "2 minutes" """
        result = format_lag_display(120)
        assert result == "2 minutes"

    def test_3661_returns_1_hour_1_minute(self):
        """3661 → "1 hour 1 minute" """
        result = format_lag_display(3661)
        assert result == "1 hour 1 minute"

    def test_90000_returns_1_day_1_hour(self):
        """90000 → "1 day 1 hour" """
        result = format_lag_display(90000)
        assert result == "1 day 1 hour"

    def test_61_seconds_singular_minute(self):
        """61 seconds → uses singular 'minute' """
        result = format_lag_display(61)
        assert result == "1 minute"

    def test_3600_seconds_1_hour_singular(self):
        """3600 seconds → singular 'hour' """
        result = format_lag_display(3600)
        assert result == "1 hour"

    def test_86400_seconds_1_day_singular(self):
        """86400 seconds → singular 'day' """
        result = format_lag_display(86400)
        assert result == "1 day"

    def test_90061_seconds_day_hour_minute(self):
        """90061 seconds = 1 day, 1 hour, 1 minute """
        result = format_lag_display(90061)
        assert result == "1 day 1 hour 1 minute"

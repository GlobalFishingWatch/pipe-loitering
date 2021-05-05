import apache_beam as beam
import datetime as dt
import time
from types import SimpleNamespace
import loitering.transforms.group_loitering_ranges as group

class TestPartitionBucketsIntoDays:
    def test_partition_buckets_into_days(self):
        buckets = [
            { "start_timestamp": dt.datetime(2019, 12, 5), "seg_id": "1" },
            { "start_timestamp": dt.datetime(2019, 12, 27), "seg_id": "1" },
            { "start_timestamp": dt.datetime(2019, 12, 31), "seg_id": "1" },
            { "start_timestamp": dt.datetime(2020, 1, 1), "seg_id": "1" },
            { "start_timestamp": dt.datetime(2020, 1, 5), "seg_id": "1" },
        ]

        result = group.partition_buckets_into_days(buckets, dt.date(2020, 1, 1))

        assert result == (
            [
                { "start_timestamp": dt.datetime(2019, 12, 5), "seg_id": "1" },
                { "start_timestamp": dt.datetime(2019, 12, 27), "seg_id": "1" },
                { "start_timestamp": dt.datetime(2019, 12, 31), "seg_id": "1" },
            ],
            [
                { "start_timestamp": dt.datetime(2020, 1, 1), "seg_id": "1" },
                { "start_timestamp": dt.datetime(2020, 1, 5), "seg_id": "1" },
            ]
        )

    def test_partition_buckets_into_days_none_before(self):
        buckets = [
            { "start_timestamp": dt.datetime(2020, 1, 1), "seg_id": "1" },
            { "start_timestamp": dt.datetime(2020, 1, 5), "seg_id": "1" },
        ]

        result = group.partition_buckets_into_days(buckets, dt.date(2020, 1, 1))

        assert result == (
            [],
            [
                { "start_timestamp": dt.datetime(2020, 1, 1), "seg_id": "1" },
                { "start_timestamp": dt.datetime(2020, 1, 5), "seg_id": "1" },
            ]
        )

    def test_partition_buckets_into_days_none_after(self):
        buckets = [
            { "start_timestamp": dt.datetime(2019, 12, 5), "seg_id": "1" },
            { "start_timestamp": dt.datetime(2019, 12, 27), "seg_id": "1" },
            { "start_timestamp": dt.datetime(2019, 12, 31), "seg_id": "1" },
        ]

        result = group.partition_buckets_into_days(buckets, dt.date(2020, 1, 1))

        assert result == (
            [
                { "start_timestamp": dt.datetime(2019, 12, 5), "seg_id": "1" },
                { "start_timestamp": dt.datetime(2019, 12, 27), "seg_id": "1" },
                { "start_timestamp": dt.datetime(2019, 12, 31), "seg_id": "1" },
            ],
            []
        )


class TestCalculateSlowGroups:
    def test_calculate_slow_groups_starts_beginning_of_today(self):
        segment = (
            "SEG_ID",
            [
                { "start_timestamp": dt.datetime(2020, 1, 1), "slow": True, "msgs": [1, 2] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": True, "msgs": [3, 4] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": True, "msgs": [5, 6] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": False, "msgs": [7, 8] },
            ]
        )

        window_end = beam.utils.timestamp.Timestamp(
            time.mktime(dt.datetime(2020, 1, 3).timetuple())
        )

        window = SimpleNamespace(end=window_end)

        result = group.calculate_slow_groups(segment, window)

        assert result == [
            [2, 3, 4, 5, 6]
        ]

    def test_calculate_slow_groups_starts_mid_day(self):
        segment = (
            "SEG_ID",
            [
                { "start_timestamp": dt.datetime(2020, 1, 1), "slow": True, "msgs": [1, 2] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": False, "msgs": [3, 4] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": True, "msgs": [5, 6] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": True, "msgs": [7, 8] },
            ]
        )

        window_end = beam.utils.timestamp.Timestamp(
            time.mktime(dt.datetime(2020, 1, 3).timetuple())
        )

        window = SimpleNamespace(end=window_end)

        result = group.calculate_slow_groups(segment, window)

        assert result == [
            [4, 5, 6, 7, 8]
        ]

    def test_calculate_slow_groups_multiple_single_day(self):
        segment = (
            "SEG_ID",
            [
                { "start_timestamp": dt.datetime(2020, 1, 1), "slow": False, "msgs": [1, 2] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": True, "msgs": [3, 4] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": False, "msgs": [5, 6] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": True, "msgs": [7, 8] },
                { "start_timestamp": dt.datetime(2020, 1, 2), "slow": False, "msgs": [9, 10] },
            ]
        )

        window_end = beam.utils.timestamp.Timestamp(
            time.mktime(dt.datetime(2020, 1, 3).timetuple())
        )

        window = SimpleNamespace(end=window_end)

        result = group.calculate_slow_groups(segment, window)

        assert result == [
            [2, 3, 4],
            [6, 7, 8],
        ]

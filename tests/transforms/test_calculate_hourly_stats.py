import loitering.transforms.calculate_hourly_stats as stats
import datetime as dt
import pytest

class TestConvertGroupToHourlyBucket:
    def test_sorts_mesasges(self):
        group = (("SEG", "HOUR"), [
            {"ssvid": "1", "timestamp": dt.datetime(2020, 2, 1)},
            {"ssvid": "1", "timestamp": dt.datetime(2020, 1, 1)},
            {"ssvid": "1", "timestamp": dt.datetime(2020, 1, 17)},
        ])

        result = stats.convert_group_to_hourly_bucket(group)

        assert result == {
            "seg_id": "SEG",
            "ssvid": "1",
            "start_timestamp": dt.datetime(2020, 1, 1),
            "end_timestamp": dt.datetime(2020, 2, 1),
            "msgs": [
                {"ssvid": "1", "timestamp": dt.datetime(2020, 1, 1)},
                {"ssvid": "1", "timestamp": dt.datetime(2020, 1, 17)},
                {"ssvid": "1", "timestamp": dt.datetime(2020, 2, 1)},
            ]
        }

class TestCalculateTotalHours:
    def test_calculate_total_hours(self):
        msgs = [
            {"ssvid": "1", "hours": 1},
            {"ssvid": "1", "hours": 0},
            {"ssvid": "1", "hours": None},
            {"ssvid": "1", "hours": 2},
        ]

        result = stats.calculate_total_hours(msgs)

        assert result == pytest.approx(3)

    def test_calculate_total_hours_with_zeroes_or_nones(self):
        msgs = [
            {"ssvid": "1", "hours": 0},
            {"ssvid": "1", "hours": None},
        ]

        result = stats.calculate_total_hours(msgs)

        assert result is None

class TestCalculateTotalDistance:
    def test_calculate_total_distance(self):
        msgs = [
            {"ssvid": "1", "meters_to_prev": 1},
            {"ssvid": "1", "meters_to_prev": 0},
            {"ssvid": "1", "meters_to_prev": None},
            {"ssvid": "1", "meters_to_prev": 2},
        ]

        result = stats.calculate_total_distance(msgs)

        assert result == pytest.approx(3)

    def test_calculate_total_distance_with_zeroes(self):
        msgs = [
            {"ssvid": "1", "meters_to_prev": 0},
            {"ssvid": "1", "meters_to_prev": None},
        ]

        result = stats.calculate_total_distance(msgs)

        assert result == pytest.approx(0)

    def test_calculate_total_distance_with_nones(self):
        msgs = [
            {"ssvid": "1", "meters_to_prev": None},
            {"ssvid": "1", "meters_to_prev": None},
        ]

        result = stats.calculate_total_distance(msgs)

        assert result is None

class TestCalculateAvgSpeedInKnots:
    def test_calculate_avg_speed_in_knots(self):
        bucket = {
            "hours": 5, "meters_to_prev": 1852
        }

        result = stats.calculate_avg_speed_in_knots(bucket)

        assert result == pytest.approx(0.2)

    def test_calculate_avg_speed_in_knots_zero_hours(self):
        bucket = {
            "hours": 0, "meters_to_prev": 1852
        }

        result = stats.calculate_avg_speed_in_knots(bucket)

        assert result is None

    def test_calculate_avg_speed_in_knots_none_hours(self):
        bucket = {
            "hours": None, "meters_to_prev": 1852
        }

        result = stats.calculate_avg_speed_in_knots(bucket)

        assert result is None

    def test_calculate_avg_speed_in_knots_zero_distance(self):
        bucket = {
            "hours": 5, "meters_to_prev": 0
        }

        result = stats.calculate_avg_speed_in_knots(bucket)

        assert result == pytest.approx(0)

    def test_calculate_avg_speed_in_knots_none_distance(self):
        bucket = {
            "hours": 5, "meters_to_prev": None
        }

        result = stats.calculate_avg_speed_in_knots(bucket)

        assert result is None

class TestCalculateIsSlow:
    def test_calculate_is_slow_when_it_is(self):
        result = stats.calculate_is_slow({"avg_speed_knots": 1.5}, slow_threshold=2)
        assert result

    def test_calculate_is_slow_when_it_isnt(self):
        result = stats.calculate_is_slow({"avg_speed_knots": 2.5}, slow_threshold=2)
        assert not result

    def test_calculate_is_slow_when_zero(self):
        result = stats.calculate_is_slow({"avg_speed_knots": 0}, slow_threshold=2)
        assert result

    def test_calculate_is_slow_when_none(self):
        result = stats.calculate_is_slow({"avg_speed_knots": None}, slow_threshold=2)
        assert not result

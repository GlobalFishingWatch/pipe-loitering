import loitering.transforms.calculate_loitering_stats as stats
import pytest

class TestCalculateAvgSpeedInKnots:
    def test_calculate_avg_speed_in_knots_zero_hours(self):
        msgs = [
            { "hours": 0, "meters_to_prev": 1 },
        ]

        result = stats.calculate_avg_speed_in_knots(msgs)

        assert result == None

    def test_calculate_avg_speed_in_knots_none_hours(self):
        msgs = [
            { "hours": None, "meters_to_prev": 1 },
        ]

        result = stats.calculate_avg_speed_in_knots(msgs)

        assert result == None

    def test_calculate_avg_speed_in_knots_none_distance(self):
        msgs = [
            { "hours": 1, "meters_to_prev": None },
        ]

        result = stats.calculate_avg_speed_in_knots(msgs)

        assert result == None

    def test_calculate_avg_speed_in_knots_zero_distance(self):
        msgs = [
            { "hours": 1, "meters_to_prev": 0 },
        ]

        result = stats.calculate_avg_speed_in_knots(msgs)

        assert result == pytest.approx(0)

    def test_calculate_avg_speed_in_knots_complex(self):
        msgs = [
            { "hours": 0, "meters_to_prev": 1 },
            { "hours": None, "meters_to_prev": 2 },
            { "hours": 1, "meters_to_prev": None },
            { "hours": 1, "meters_to_prev": 0 },
            { "hours": 1, "meters_to_prev": 1852 },
        ]

        result = stats.calculate_avg_speed_in_knots(msgs)

        assert result == pytest.approx(1855 / 1852 / 3)

class TestCalculateAvgDistanceFromShoreNM:
    def test_calculate_avg_distance_from_shore_nm_no_hours_no_distance(self):
        msgs = [
            { "hours": 0, "distance_from_shore_m": None },
        ]

        result = stats.calculate_avg_distance_from_shore_nm(msgs)

        assert result == None

    def test_calculate_avg_distance_from_shore_nm_hours_no_distance(self):
        msgs = [
            { "hours": 1, "distance_from_shore_m": None },
        ]

        result = stats.calculate_avg_distance_from_shore_nm(msgs)

        assert result == None

    def test_calculate_avg_distance_from_shore_nm_complex_with_hours(self):
        msgs = [
            { "hours": 0, "distance_from_shore_m": None },
            { "hours": None, "distance_from_shore_m": 1 },
            { "hours": 1, "distance_from_shore_m": None },
            { "hours": 3, "distance_from_shore_m": 1852 },
            { "hours": 2, "distance_from_shore_m": 1852 },
        ]

        result = stats.calculate_avg_distance_from_shore_nm(msgs)

        assert result == 1

    def test_calculate_avg_distance_from_shore_nm_complex_without_hours(self):
        msgs = [
            { "hours": 0, "distance_from_shore_m": None },
            { "hours": None, "distance_from_shore_m": 1 },
            { "hours": 3, "distance_from_shore_m": 1852 },
            { "hours": 2, "distance_from_shore_m": 1852 },
        ]

        result = stats.calculate_avg_distance_from_shore_nm(msgs)

        assert result == 1

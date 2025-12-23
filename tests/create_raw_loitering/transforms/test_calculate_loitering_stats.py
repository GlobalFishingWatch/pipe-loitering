import pipe_loitering.create_raw_loitering.transforms.calculate_loitering_stats as stats
import pytest
import datetime as dt

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

class TestConvertToLoiteringDailyEvents:
    def test_only_one_message(self):
        msgs = [{
            'ssvid': '503040990',
            'seg_id': '503040990-2023-10-27T00:31:23.000000Z-1',
            'timestamp': dt.datetime(2023, 10, 28, 0, 14, 54, tzinfo=dt.timezone.utc),
            'meters_to_prev': 20167.090529863097,
            'hours': 23.725277777777777,
            'lat': -20.174032,
            'lon': 148.89871,
            'distance_from_shore_m': 1000.0
        }]
        result = stats.has_more_than_one_messages(msgs)

        assert result == False

    def test_convert_to_loitering_daily_events(self):
        msgs = [{
            'ssvid': '503040990',
            'seg_id': '503040990-2023-10-27T00:31:23.000000Z-1',
            'timestamp': dt.datetime(2023, 10, 28, 0, 14, 54, tzinfo=dt.timezone.utc),
            'meters_to_prev': 20167.090529863097,
            'hours': 23.725277777777777,
            'lat': -20.174032,
            'lon': 148.89871,
            'distance_from_shore_m': 1000.0
        },{
            'ssvid': '503040990',
            'seg_id': '503040990-2023-10-27T01:31:23.000000Z-1',
            'timestamp': dt.datetime(2023, 10, 28, 1, 14, 54, tzinfo=dt.timezone.utc),
            'meters_to_prev': 20267.090529863097,
            'hours': 23.725277777777777,
            'lat': -20.174042,
            'lon': 148.89871,
            'distance_from_shore_m': 1003.0
        }]

        result = stats.convert_to_loitering_daily_event(msgs)

        assert result == {
            'avg_distance_from_shore_nm': 0.5407667386609072,
            'avg_speed_knots': 0.4612529100967032,
            'end_lat': -20.174042,
            'end_lon': 148.89871,
            'loitering_end_timestamp': dt.datetime(2023, 10, 28, 1, 14, 54, tzinfo=dt.timezone.utc),
            'loitering_hours': 23.725277777777777,
            'loitering_start_timestamp': dt.datetime(2023, 10, 28, 0, 14, 54, tzinfo=dt.timezone.utc),
            'seg_id': '503040990-2023-10-27T00:31:23.000000Z-1',
            'ssvid': '503040990',
            'start_lat': -20.174032,
            'start_lon': 148.89871,
            'tot_distance_nm': 10.943353417852643
        }


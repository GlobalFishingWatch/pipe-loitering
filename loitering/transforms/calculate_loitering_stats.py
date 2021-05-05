import apache_beam as beam
from loitering.transforms.calculate_hourly_stats import calculate_total_hours, calculate_total_distance

def calculate_avg_speed_in_knots(msgs):
    hours = calculate_total_hours(msgs)
    distance_m = calculate_total_distance(msgs)
    if hours and distance_m is not None:
        return  distance_m / hours / 1852
    else:
        return None

def calculate_avg_distance_from_shore_nm(msgs):
    hours = calculate_total_hours(msgs)

    if hours:
        return sum([msg["distance_from_shore_m"] * msg["hours"] for msg in msgs]) / hours / 1852
    else:
        return sum([msg["distance_from_shore_m"] for msg in msgs]) / len(msgs) / 1852

def calculate_positions(msgs):
    position_pairs = ["{} {}".format(msg["lon"], msg["lat"]) for msg in msgs]
    coordinates = ",".join(position_pairs)
    return "LINESTRING ({})".format(coordinates)


def convert_to_loitering_daily_event(msgs):
    return {
        "ssvid": msgs[0]["ssvid"],
        "seg_id": msgs[0]["seg_id"],
        "loitering_start_timestamp": msgs[0]["timestamp"],
        "loitering_end_timestamp": msgs[-1]["timestamp"],
        "avg_speed_knots": calculate_avg_speed_in_knots(msgs[1:]),
        "loitering_hours": calculate_total_hours(msgs[1:]),
        "tot_distance_nm": calculate_total_distance(msgs[1:]) / 1852,
        "avg_distance_from_shore_nm": calculate_avg_distance_from_shore_nm(msgs),
        "start_lon": msgs[0]["lon"],
        "start_lat": msgs[0]["lat"],
        "end_lon": msgs[-1]["lon"],
        "end_lat": msgs[-1]["lat"],
        "positions": calculate_positions(msgs),
    }

class CalculateLoiteringStats(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | self.convert_to_loitering_daily_event()
        )

    def convert_to_loitering_daily_event(self):
        return beam.Map(convert_to_loitering_daily_event)

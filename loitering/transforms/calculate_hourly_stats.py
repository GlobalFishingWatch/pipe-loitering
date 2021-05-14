import apache_beam as beam

def convert_group_to_hourly_bucket(group):
    key, msgs = group
    sorted_messages = sorted(msgs, key=lambda msg: msg["timestamp"])

    return {
        "seg_id": key[0],
        "ssvid": sorted_messages[0]["ssvid"],
        "start_timestamp": sorted_messages[0]["timestamp"],
        "end_timestamp": sorted_messages[-1]["timestamp"],
        "msgs": sorted_messages,
    }

def calculate_total_hours(msgs):
    hours = [msg["hours"] for msg in msgs if msg["hours"]]
    return sum(hours) if hours else None

def calculate_total_distance(msgs):
    distances = [msg["meters_to_prev"] for msg in msgs if msg["meters_to_prev"] is not None]
    return sum(distances) if distances else None

def calculate_avg_speed_in_knots(bucket):
    if bucket['hours'] and bucket["meters_to_prev"] is not None:
        return bucket['meters_to_prev'] / bucket['hours'] / 1852
    else:
        return None

def calculate_is_slow(bucket, slow_threshold):
    return bucket["avg_speed_knots"] and bucket["avg_speed_knots"] < slow_threshold

class CalculateHourlyStats(beam.PTransform):
    def __init__(self, slow_threshold):
        self.slow_threshold = slow_threshold

    def expand(self, pcoll):
        return (
            pcoll
            | self.group_by_seg_id_and_hourly_bucket()
            | self.convert_group_to_hourly_bucket()
            | self.calculate_total_hours()
            | self.calculate_total_distance()
            | self.calculate_avg_speed_in_knots()
            | self.calculate_is_slow()
        )

    def group_by_seg_id_and_hourly_bucket(self):
        return beam.GroupBy(
            seg_id=lambda msg: msg['seg_id'],
            hourly_bucket=lambda msg: msg["timestamp"].strftime("%Y-%m-%d-%H")
        )

    def convert_group_to_hourly_bucket(self):
        return beam.Map(convert_group_to_hourly_bucket)

    def calculate_total_hours(self):
        return beam.Map(lambda bucket: dict(**bucket, hours=calculate_total_hours(bucket["msgs"])))

    def calculate_total_distance(self):
        return beam.Map(lambda bucket: dict(**bucket, meters_to_prev=calculate_total_distance(bucket["msgs"])))

    def calculate_avg_speed_in_knots(self):
        return beam.Map(lambda bucket: dict(**bucket, avg_speed_knots=calculate_avg_speed_in_knots(bucket)))

    def calculate_is_slow(self):
        return beam.Map(lambda bucket: dict(**bucket, slow=calculate_is_slow(bucket, self.slow_threshold)))

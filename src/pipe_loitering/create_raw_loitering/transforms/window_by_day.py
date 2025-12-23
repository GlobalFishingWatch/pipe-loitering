import apache_beam as beam
import time

DAY_IN_SECONDS = 24 * 60 * 60


def to_unix_time(dt):
    return time.mktime(dt.timetuple())


class SlidingWindowByDay(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | self.assign_timestamps() | self.apply_windowing()

    def assign_timestamps(self):
        return beam.Map(
            lambda bucket: beam.window.TimestampedValue(
                bucket, to_unix_time(bucket["start_timestamp"])
            )
        )

    def apply_windowing(self):
        return beam.WindowInto(beam.window.SlidingWindows(2 * DAY_IN_SECONDS, DAY_IN_SECONDS))

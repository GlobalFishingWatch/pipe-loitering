import apache_beam as beam
import itertools as it
import datetime as dt

def window_current_day(window):
    return window.end.to_utc_datetime().date() - dt.timedelta(days=1)

def partition_buckets_into_days(buckets, split_date):
    yesterday_buckets = []
    today_buckets = []

    for bucket in buckets:
        if bucket["start_timestamp"].date() < split_date:
            yesterday_buckets.append(bucket)
        else:
            today_buckets.append(bucket)

    return (yesterday_buckets, today_buckets)

def is_not_buffer_window(buffer_date, window):
    current_day = window_current_day(window)
    return buffer_date != current_day

def calculate_slow_groups(segment, window=beam.DoFn.WindowParam):
    current_day = window_current_day(window)
    buckets = sorted(segment[1], key=lambda bucket: bucket["start_timestamp"])
    yesterday_buckets, today_buckets = partition_buckets_into_days(buckets, current_day)

    result = []
    current_group = []
    for i in range(0, len(today_buckets)):
        current_bucket = today_buckets[i]

        if current_bucket["slow"] and not current_group:
            # We are starting a new slow group
            previous_bucket = None
            if i == 0 and yesterday_buckets:
                previous_bucket = yesterday_buckets[-1]
            elif i > 0:
                previous_bucket = today_buckets[i - 1]

            if previous_bucket:
                current_group.append(previous_bucket["msgs"][-1])

            current_group.extend(current_bucket["msgs"])

        elif current_bucket["slow"] and current_group:
            # We are adding a new bucket to an existing slow group
            current_group.extend(current_bucket["msgs"])

        elif not current_bucket["slow"] and current_group:
            # We need to close an existing slow group
            result.append(current_group)
            current_group = []

    if current_group:
        result.append(current_group)

    return result


class GroupLoiteringRanges(beam.PTransform):
    def __init__(self, date_range):
        self.buffer_date = date_range[0].date()

    def expand(self, pcoll):
        return (
            pcoll
            | self.group_by_seg_id()
            | self.filter_buffer_window()
            | self.calculate_slow_groups()
        )

    def group_by_seg_id(self):
        return beam.GroupBy(seg_id=lambda bucket: bucket['seg_id'])

    def filter_buffer_window(self):
        return beam.Filter(lambda _, window=beam.DoFn.WindowParam: is_not_buffer_window(self.buffer_date, window))

    def calculate_slow_groups(self):
        return beam.FlatMap(calculate_slow_groups)


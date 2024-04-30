import apache_beam as beam
import datetime as dt
from loitering.create_raw_loitering.options import LoiteringOptions
from loitering.create_raw_loitering.transforms.read_source import ReadSource
from loitering.create_raw_loitering.transforms.calculate_hourly_stats import CalculateHourlyStats
from loitering.create_raw_loitering.transforms.window_by_day import SlidingWindowByDay
from loitering.create_raw_loitering.transforms.group_loitering_ranges import GroupLoiteringRanges
from loitering.create_raw_loitering.transforms.calculate_loitering_stats import CalculateLoiteringStats
from loitering.create_raw_loitering.transforms.write_sink import WriteSink

def parse_yyyy_mm_dd_param(value):
    return dt.datetime.strptime(value, "%Y-%m-%d")

class LoiteringPipeline:
    def __init__(self, options):
        self.pipeline = beam.Pipeline(options=options)

        params = options.view_as(LoiteringOptions)

        start_date = parse_yyyy_mm_dd_param(params.start_date)
        end_date = parse_yyyy_mm_dd_param(params.end_date)
        start_date_with_buffer = start_date - dt.timedelta(days=1)
        date_range = (start_date_with_buffer, end_date)

        (
            self.pipeline
            | ReadSource(date_range=date_range, source_table=params.source, source_timestamp_field=params.source_timestamp_field)
            | CalculateHourlyStats(slow_threshold=params.slow_threshold)
            | SlidingWindowByDay()
            | GroupLoiteringRanges(date_range=date_range)
            | CalculateLoiteringStats()
            | WriteSink(sink_table=params.sink)
        )

    def run(self):
        return self.pipeline.run()

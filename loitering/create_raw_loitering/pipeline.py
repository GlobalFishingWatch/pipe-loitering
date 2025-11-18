from apache_beam.options.pipeline_options import GoogleCloudOptions
from loitering.create_raw_loitering.options import LoiteringOptions
from loitering.create_raw_loitering.transforms.calculate_hourly_stats import CalculateHourlyStats
from loitering.create_raw_loitering.transforms.calculate_loitering_stats import CalculateLoiteringStats
from loitering.create_raw_loitering.transforms.group_loitering_ranges import GroupLoiteringRanges
from loitering.create_raw_loitering.transforms.read_source import ReadSource
from loitering.create_raw_loitering.transforms.window_by_day import SlidingWindowByDay
from loitering.create_raw_loitering.transforms.write_sink import WriteSink
from loitering.utils.ver import get_pipe_ver
import apache_beam as beam
import datetime as dt
import logging
from google.cloud import bigquery


logger = logging.getLogger(__name__)


def parse_yyyy_mm_dd_param(value):
    return dt.datetime.strptime(value, "%Y-%m-%d")

list_to_dict = lambda labels: {x.split('=')[0]:x.split('=')[1] for x in labels}

def get_description(opts):
    return f"""
Created by pipe-loitering: {get_pipe_ver()}.
* Daily static loitering events. This is an intermediate internal table that's
used to later aggregate into actual loitering events.
* https://github.com/GlobalFishingWatch/pipe-loitering
* Source: {opts.source}
* Threshold speed (knots): {opts.slow_threshold}
* Date: {opts.start_date}, {opts.end_date}
"""


DELETE_QUERY = """
DELETE FROM `{table}`
WHERE DATE(loitering_start_timestamp) = '{start_date}'
"""


class LoiteringPipeline:
    def __init__(self, options):
        self.pipeline = beam.Pipeline(options=options)

        params = options.view_as(LoiteringOptions)
        gCloudParams = options.view_as(GoogleCloudOptions)

        start_date = parse_yyyy_mm_dd_param(params.start_date)
        end_date = parse_yyyy_mm_dd_param(params.end_date)
        start_date_with_buffer = start_date - dt.timedelta(days=1)
        date_range = (start_date_with_buffer, end_date)
        labels = list_to_dict(gCloudParams.labels)

        bq = bigquery.Client(project=gCloudParams.project)

        # Ensure we delete any existing rows from the date to be processed.
        # Needed to maintain consistency if are re-processing dates.
        logger.info("Deleting existing rows with start_date {}".format(start_date))
        bq.query(DELETE_QUERY.format(table=params.sink, start_date=start_date))

        (
            self.pipeline
            | ReadSource(date_range=date_range, source_table=params.source, labels=labels, source_timestamp_field=params.source_timestamp_field)
            | CalculateHourlyStats(slow_threshold=params.slow_threshold)
            | SlidingWindowByDay()
            | GroupLoiteringRanges(date_range=date_range)
            | CalculateLoiteringStats()
            | WriteSink(sink_table=params.sink, description=get_description(params))
        )

    def run(self):
        return self.pipeline.run()

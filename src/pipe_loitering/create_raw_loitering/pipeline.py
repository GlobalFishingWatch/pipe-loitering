import logging
import datetime as dt
from types import SimpleNamespace

from google.cloud import bigquery
from apache_beam.runners import PipelineState
from gfw.common.beam.pipeline import Pipeline

from pipe_loitering.create_raw_loitering.transforms.calculate_hourly_stats import (
    CalculateHourlyStats,
)
from pipe_loitering.create_raw_loitering.transforms.calculate_loitering_stats import (
    CalculateLoiteringStats,
)
from pipe_loitering.create_raw_loitering.transforms.group_loitering_ranges import (
    GroupLoiteringRanges,
)

from pipe_loitering.create_raw_loitering.transforms.read_source import ReadSource
from pipe_loitering.create_raw_loitering.transforms.window_by_day import SlidingWindowByDay
from pipe_loitering.create_raw_loitering.transforms.write_sink import WriteSink
from pipe_loitering.utils.ver import get_pipe_ver


logger = logging.getLogger(__name__)


def parse_yyyy_mm_dd_param(value):
    return dt.datetime.strptime(value, "%Y-%m-%d")


def get_description(opts):
    return f"""
Created by pipe-loitering: {get_pipe_ver()}.
* Daily static loitering events. This is an intermediate internal table that's
used to later aggregate into actual loitering events.
* https://github.com/GlobalFishingWatch/pipe-loitering
* Source: {opts.bq_input}
* Threshold speed (knots): {opts.slow_threshold}
"""


DELETE_QUERY = """
    DELETE FROM `{table}`
    WHERE DATE({partitioning_field})
    BETWEEN '{start_date}' AND '{end_date}'
"""


# TODO: refactor to use gfw.common.beam.pipeline package.
def run(args: SimpleNamespace):
    pipeline = LoiteringPipeline(args)
    result = pipeline.run()
    result.wait_until_finish()


class LoiteringPipeline:
    def __init__(self, options):
        pipeline = Pipeline(
            unparsed_args=options.unknown_unparsed_args,
            **options.unknown_parsed_args
        )

        self.pipeline = pipeline.pipeline

        start_date_str, end_date_str = options.date_range

        start_date = parse_yyyy_mm_dd_param(start_date_str).date()
        end_date = parse_yyyy_mm_dd_param(end_date_str).date()

        start_date_with_buffer = start_date - dt.timedelta(days=1)
        date_range = (start_date_with_buffer, end_date)

        (
            self.pipeline
            | ReadSource(
                date_range=date_range,
                source_table=options.bq_input,
                labels=options.labels,
                source_timestamp_field=options.source_timestamp_field,
            )
            | CalculateHourlyStats(slow_threshold=options.slow_threshold)
            | SlidingWindowByDay()
            | GroupLoiteringRanges(date_range=date_range)
            | CalculateLoiteringStats()
            | WriteSink(sink_table=options.bq_output)
        )

        self.options = options
        self.gcloud_params = pipeline.cloud_options
        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        bq = bigquery.Client(project=self.gcloud_params.project)

        # Ensure we delete any existing rows from the date to be processed.
        # Needed to maintain consistency if are re-processing dates.
        logger.info(
            "Deleting events from {} whose end_date is in the range [{},{}] (inclusive)...".format(
                self.options.bq_output, self.start_date, self.end_date
            )
        )

        bq.query(
            DELETE_QUERY.format(
                table=self.options.bq_output,
                partitioning_field="loitering_end_timestamp",
                start_date=self.start_date,
                end_date=self.end_date,
            )
        )

        result = self.pipeline.run()
        result.wait_until_finish()

        if result.state == PipelineState.DONE:
            logger.info("Updating table description...")
            table = bq.get_table(self.options.bq_output)
            table.description = get_description(self.options)
            bq.update_table(table, ["description"])
            logger.info("Ok.")

        return result

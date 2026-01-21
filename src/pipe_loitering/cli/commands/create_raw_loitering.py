from typing import Any
from types import SimpleNamespace

import argparse

from gfw.common.cli import Command, Option

from pipe_loitering.create_raw_loitering.pipeline import run

HELP_BQ_INPUT = (
    "Source table to read messages from, in the format 'PROJECT.DATASET.TABLE'. "
    "Usually, this is the pre-thinned and filtered gfw_research.pipe_vXYZ table, "
    "such as gfw_research.pipe_v20201001."
)

HELP_BQ_OUTPUT = "Destination table to write messages to, in the format 'PROJECT.DATASET.TABLE'."
HELP_DATE_RANGE = "Detect raw loitering within this date range, e.g., «2024-01-01,2024-01-02»."
HELP_SLOW_THRESHOLD = "Threshold speed in knots to consider an hour to be slow."
HELP_TIMESTAMP_FIELD = "Field used in source to filter records by start and end date."


ERROR_DATE_RANGE = "Must be a comma-separated string with two dates in ISO format."


# TODO: mopve date_range validation to gfw.common lib.
def date_range(date_str: str) -> tuple[str, str]:
    parts = date_str.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(ERROR_DATE_RANGE)

    return tuple(parts)


class CreateRawLoitering(Command):
    @property
    def name(cls):
        return "create-raw-loitering"

    @property
    def description(self):
        return "Detects raw loitering events."

    @property
    def options(self):
        return [
            Option("--bq-input", type=str, required=True, help=HELP_BQ_INPUT),
            Option("--bq-output", type=str, required=True, help=HELP_BQ_OUTPUT),
            Option("--date-range", type=date_range, required=True, help=HELP_DATE_RANGE),
            Option("--slow-threshold", type=float, default=2, help=HELP_SLOW_THRESHOLD),
            Option(
                "--source-timestamp-field",
                type=str, default="timestamp", help=HELP_TIMESTAMP_FIELD),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)

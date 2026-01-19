from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_loitering.merge_raw_loitering import run

HELP_BQ_INPUT = "Source date-partitioned table to read daily loitering events from."
HELP_BQ_OUTPUT = "Destination table to write consolidated loitering events to."


class MergeRawLoitering(Command):
    @property
    def name(cls):
        return "merge-raw-loitering"

    @property
    def description(self):
        return "Merges the raw loitering events."

    @property
    def options(self):
        return [
            Option("--bq-input", type=str, required=True, help=HELP_BQ_INPUT),
            Option("--bq-output", type=str, required=True, help=HELP_BQ_OUTPUT),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)

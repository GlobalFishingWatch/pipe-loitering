#! /usr/bin/env python
import sys
import logging

from gfw.common.cli import CLI, Option
from gfw.common.cli.actions import NestedKeyValueAction
from gfw.common.logging import LoggerConfig
from gfw.common.cli.formatting import default_formatter

from pipe_loitering.cli.commands.create_raw_loitering import CreateRawLoitering
from pipe_loitering.cli.commands.merge_raw_loitering import MergeRawLoitering

from pipe_loitering.version import __version__


logging.basicConfig(level=logging.INFO)

HELP_LABELS = "Labels to audit costs over the queries."


def run(args):
    loitering_cli = CLI(
        name="pipe-loitering",
        description="Tools for creating loitering events.",
        formatter=default_formatter(max_pos=120),
        subcommands=[
            CreateRawLoitering,
            MergeRawLoitering
        ],
        options=[  # Common options for all subcommands.
            Option(
                "--labels", type=str, nargs="*", action=NestedKeyValueAction, help=HELP_LABELS
            ),
        ],
        version=__version__,
        examples=[
        ],
        logger_config=LoggerConfig(
            warning_level=[
                "apache_beam.runners.portability",
                "apache_beam.runners.worker",
                "apache_beam.transforms.core",
                "apache_beam.io.filesystem",
                "apache_beam.io.gcp.bigquery_tools",
                "urllib3"
            ]
        ),
        allow_unknown=True
    )

    return loitering_cli.execute(args)


def main():
    run(sys.argv[1:])


if __name__ == "__main__":
    main()

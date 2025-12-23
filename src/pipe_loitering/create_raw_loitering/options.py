from apache_beam.options.pipeline_options import PipelineOptions


class LoiteringOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        required = parser.add_argument_group("Required")
        required.add_argument(
            "--source",
            required=True,
            help=(
                "Source table to read messages from, in the format 'PROJECT.DATASET.TABLE'. "
                "Usually, this is the pre-thinned and filtered gfw_research.pipe_vXYZ table, "
                "such as gfw_research.pipe_v20201001.",
            )
        )

        required.add_argument(
            "--sink",
            required=True,
            help="Destination table to write messages to, in the format 'PROJECT.DATASET.TABLE'.",
        )

        required.add_argument(
            "--start_date",
            required=True,
            help="Read the source table for messages after start date in format YYYY-MM-DD",
        )

        required.add_argument(
            "--end_date",
            required=True,
            help="Read the source table for messages before end date in format YYYY-MM-DD",
        )

        optional = parser.add_argument_group("Optional")
        optional.add_argument(
            "--slow_threshold",
            default=2,
            type=float,
            help="Threshold speed in knots to consider an hour to be slow",
        )

        optional.add_argument(
            "--wait_for_job",
            default=False,
            action="store_true",
            help="Wait until the job finishes before returning.",
        )

        required.add_argument(
            "--source_timestamp_field",
            required=False,
            help="Field used in source to filter records by start and end date.",
            default="timestamp",
        )

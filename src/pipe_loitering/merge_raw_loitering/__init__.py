from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader
from pipe_loitering.utils.ver import get_pipe_ver
import logging

from importlib.resources import files

template_dir = files("pipe_loitering").joinpath("merge_raw_loitering")

templates = Environment(
    loader=FileSystemLoader(str(template_dir)),
)

TABLE_SCHEMA = {
    "fields": [
        {
            "name": "ssvid",
            "type": "STRING",
            "mode": "NULLABLE",
            "description": "SSVID of the vessel involved in the loitering event",
        },
        {
            "name": "seg_id",
            "type": "STRING",
            "mode": "NULLABLE",
            "description": "Segment id of the segment where the loitering event was detected",
        },
        {
            "name": "loitering_start_timestamp",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
            "description": "Starting timestamp for the loitering event",
        },
        {
            "name": "loitering_end_timestamp",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
            "description": "Ending timestamp for the loitering event",
        },
        {
            "name": "loitering_hours",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": "Amount of hours the loitering event lasted",
        },
        {
            "name": "tot_distance_nm",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": (
                "Total distance in nautical miles the vessel moved during the loitering event"
            )
        },
        {
            "name": "avg_speed_knots",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": (
                "Average speed (knots) for all the hourly buckets contained in the loitering event"
            )
        },
        {
            "name": "avg_distance_from_shore_nm",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": (
                "Weighted average distance from shore in nautical miles during the loitering event"
            )
        },
        {
            "name": "start_lon",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": "Longitude for the starting position of the loitering event",
        },
        {
            "name": "start_lat",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": "Latitude for the starting position of the loitering event",
        },
        {
            "name": "end_lon",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": "Longitude for the ending position of the loitering event",
        },
        {
            "name": "end_lat",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": "Latitude for the ending position of the loitering event",
        },
    ],
}

SCHEMA_SCHEMAFIELDS = map(
    lambda x: bigquery.schema.SchemaField(
        x["name"], x["type"], x["mode"], description=x["description"]
    ),
    TABLE_SCHEMA["fields"],
)

bq_client = bigquery.Client()


def get_table(destination_table: str) -> bigquery.table.Table:
    dest_table_parts = destination_table.split(".")
    dataset_ref = bigquery.DatasetReference(dest_table_parts[0], dest_table_parts[1])
    return bq_client.get_table(dataset_ref.table(dest_table_parts[2]))  # API request


def run(options):
    query_template = templates.get_template("aggregate.sql.j2")
    query = query_template.render(
        source_daily_partitioned_table=options.bq_input,
    )

    logging.info("Running the following query to push data to %s", options.bq_output)
    logging.info(query)

    job = bq_client.query(
        query,
        bigquery.QueryJobConfig(
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            destination=options.bq_output,
            clustering_fields=["loitering_start_timestamp", "ssvid"],
            time_partitioning=bigquery.table.TimePartitioning(
                type_=bigquery.table.TimePartitioningType.MONTH,
                field="loitering_start_timestamp",
            ),
            labels=options.labels,
        ),
    )

    logging.info("Waiting for query job to be done")
    job.result()

    table = get_table(options.bq_output)
    table.schema = list(SCHEMA_SCHEMAFIELDS)
    table.description = f"""
Created by pipe-loitering: {get_pipe_ver()}.
* Consolidated loitering events
* https://github.com/GlobalFishingWatch/pipe-loitering
* Source: {options.bq_input}
"""
    table.require_partition_filter = True
    table.labels = options.labels
    bq_client.update_table(
        table, ["schema", "description", "require_partition_filter", "labels"]
    )  # API request

    logging.info("Done")

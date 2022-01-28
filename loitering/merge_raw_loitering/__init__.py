import logging
import argparse
from jinja2 import Environment, FileSystemLoader
from google.cloud import bigquery

templates = Environment(
    loader=FileSystemLoader("loitering/merge_raw_loitering"),
)

bq_client = bigquery.Client()

parser = argparse.ArgumentParser()
parser.add_argument('--source',
                    help='Source date-sharded table to read daily loitering events from',
                    required=True)
parser.add_argument('--destination',
                    help='Destination date-partitioned, clustered table to write consolidated loitering events to',
                    required=True)

def set_destination_table_description(destination_table:str, description:str):
    dest_table_parts = destination_table.split('.')
    dataset_ref = bigquery.DatasetReference(dest_table_parts[0], dest_table_parts[1])
    table_ref = dataset_ref.table(dest_table_parts[2])
    table = bq_client.get_table(table_ref)
    table.description = description
    table = bq_client.update_table(table, ["description"])


def run_merge_raw_loitering(argv):
    logging.info("Running merge_raw_loitering with args %s", argv)

    options = parser.parse_args(argv)

    logging.info("Parsed options is %s", options)

    query_template = templates.get_template('aggregate.sql.j2')
    query = query_template.render(
        source_daily_partitioned_table=options.source,
    )

    logging.info("Running the following query to push data to %s", options.destination)
    logging.info(query)

    job = bq_client.query(query, bigquery.QueryJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
        destination=options.destination,
        clustering_fields=['ssvid'],
        time_partitioning=bigquery.table.TimePartitioning(
            type_=bigquery.table.TimePartitioningType.DAY,
            field='loitering_start_timestamp',
        ),
    ))

    logging.info("Waiting for query job to be done")
    job.result()

    set_destination_table_description(options.destination, "Consolidated loitering events")

    logging.info("Done")

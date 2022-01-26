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
    logging.info("Done")

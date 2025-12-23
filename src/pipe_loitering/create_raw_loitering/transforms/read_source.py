import apache_beam as beam

SOURCE_QUERY_TEMPLATE = """
    SELECT
      ssvid,
      seg_id,
      timestamp,
      meters_to_prev,
      hours,
      lat,
      lon,
      distance_from_shore_m
    FROM
      `{source_table}`
    WHERE
      DATE({source_timestamp_field}) BETWEEN '{start_date}'
      AND '{end_date}'
"""


class ReadSource(beam.PTransform):
    def __init__(self, source_table, date_range, labels, source_timestamp_field):
        self.source_table = source_table
        self.start_date, self.end_date = date_range
        self.source_timestamp_field = source_timestamp_field
        self.labels = labels

    def expand(self, pcoll):
        return pcoll | self.read_source()

    def read_source(self):
        query = SOURCE_QUERY_TEMPLATE.format(
            source_table=self.source_table,
            start_date=self.start_date,
            end_date=self.end_date,
            source_timestamp_field=self.source_timestamp_field,
        )
        return beam.io.ReadFromBigQuery(
            query=query,
            use_standard_sql=True,
            bigquery_job_labels=self.labels,
        )

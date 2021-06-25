import apache_beam as beam
import datetime as dt

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
      DATE(_PARTITIONTIME) BETWEEN '{start_date}'
      AND '{end_date}'
"""

class ReadSource(beam.PTransform):
    def __init__(self, source_table, date_range):
        self.source_table = source_table
        self.start_date, self.end_date = date_range

    def expand(self, pcoll):
        return (
            pcoll
            | self.read_source()
        )

    def read_source(self):
        query = SOURCE_QUERY_TEMPLATE.format(
            source_table=self.source_table,
            start_date=self.start_date.strftime("%Y-%m-%d"),
            end_date=self.end_date.strftime("%Y-%m-%d"),
        )
        return beam.io.ReadFromBigQuery(
            query = query,
            use_standard_sql=True,
        )

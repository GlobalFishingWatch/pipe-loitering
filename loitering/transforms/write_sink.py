import apache_beam as beam

TABLE_SCHEMA={
    "fields": [
        { "name": "ssvid", "type": "STRING", "mode": "NULLABLE" },
        { "name": "seg_id", "type": "STRING", "mode": "NULLABLE" },
        { "name": "loitering_start_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE" },
        { "name": "loitering_end_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE" },
        { "name": "avg_speed_knots", "type": "FLOAT", "mode": "NULLABLE" },
        { "name": "loitering_hours", "type": "FLOAT", "mode": "NULLABLE" },
        { "name": "tot_distance_nm", "type": "FLOAT", "mode": "NULLABLE" },
        { "name": "avg_distance_from_shore_nm", "type": "FLOAT", "mode": "NULLABLE" },
        { "name": "start_lon", "type": "FLOAT", "mode": "NULLABLE" },
        { "name": "start_lat", "type": "FLOAT", "mode": "NULLABLE" },
        { "name": "end_lon", "type": "FLOAT", "mode": "NULLABLE" },
        { "name": "end_lat", "type": "FLOAT", "mode": "NULLABLE" },
        { "name": "positions", "type": "GEOGRAPHY", "mode": "NULLABLE" },
    ],
}

class WriteSink(beam.PTransform):
    def __init__(self, sink_table):
        self.sink_table = sink_table

    def expand(self, pcoll):
        return (
            pcoll
            | self.write_sink()
        )

    def write_sink(self):
        def compute_table_for_event(event):
            table_suffix = event["loitering_end_timestamp"].strftime("%Y%m%d")
            return "{}{}".format(self.sink_table, table_suffix)

        return beam.io.WriteToBigQuery(
            compute_table_for_event,
            schema=TABLE_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

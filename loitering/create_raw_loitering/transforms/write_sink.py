import apache_beam as beam

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
            "name": "avg_speed_knots",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": "Average speed, in knots for all the hourly buckets contained in the loitering event",
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
            "description": "Total distance in nautical miles the vessel moved during the loitering event",
        },
        {
            "name": "avg_distance_from_shore_nm",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": "Weighted average distance from shore in nautical miles during the loitering event",
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

BQ_PARAMS = {
    "destinationTableProperties": {
        "description": "Daily static loitering events. This is an intermediate internal table that's used to later aggregate into actual loitering events.",
    },
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
            additional_bq_parameters=BQ_PARAMS,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

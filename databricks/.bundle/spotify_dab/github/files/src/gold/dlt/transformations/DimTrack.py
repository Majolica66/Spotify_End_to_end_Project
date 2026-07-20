import dlt

@dlt.table(
    name="dimtrack_stg"
)
def dimtrack_stg():
    df = spark.readStream.table("spotify.silver.dimtrack")
    return df

dlt.create_streaming_table(name="dimtrack")

dlt.create_auto_cdc_flow(
  target = "dimtrack",
  source = "dimtrack_stg",
  keys = ["Track_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  name = None,
  once = False
)
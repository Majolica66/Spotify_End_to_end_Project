import dlt

@dlt.table(
    name="dimuser_stg"
)
def dimuser_stg():
    df = spark.readStream.table("spotify.silver.dimuser")
    return df

dlt.create_streaming_table(name="dimuser")

dlt.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_stg",
  keys = ["User_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  name = None,
  once = False
)
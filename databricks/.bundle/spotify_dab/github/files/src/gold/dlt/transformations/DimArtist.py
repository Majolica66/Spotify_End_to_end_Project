import dlt

@dlt.table(
    name="dimartist_stg"
)
def dimartist_stg():
    df = spark.readStream.table("spotify.silver.dimartist")
    return df

dlt.create_streaming_table(name="dimartist")

dlt.create_auto_cdc_flow(
  target = "dimartist",
  source = "dimartist_stg",
  keys = ["Artist_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  name = None,
  once = False
)
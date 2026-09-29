from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc_nulls_last(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

ranked = (
    df_web_events
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "dedup_rank",
        F.col("max_web_event_duration_sec"),
    )
)

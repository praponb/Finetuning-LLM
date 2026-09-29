from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("page", "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

metrics = (
    df_web_events
    .withColumn("next_duration_sec", F.lead("duration_sec", 2).over(w_running))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w2))
    .withColumn("duration_sec_bucket", F.ntile(10).over(w3))
)

result = (
    metrics
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "cumulative_duration_sec",
        "duration_sec_bucket",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc_nulls_first())
)

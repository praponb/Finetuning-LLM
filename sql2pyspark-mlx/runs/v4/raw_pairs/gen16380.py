from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("event_type")
w_recent_2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_last())
w4 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

metrics = (
    df_web_events
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("event_type_stddev_samp_duration_sec", F.stddev_samp("duration_sec").over(w_recent))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w_recent_2))
    .withColumn("event_type_avg_duration_sec", F.avg("duration_sec").over(w_recent))
    .withColumn("duration_sec_avg_7d", F.avg("duration_sec").over(w4))
)

result = (
    metrics
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "duration_sec",
        "cumulative_duration_sec",
        "event_type_stddev_samp_duration_sec",
        "duration_sec_percent_rank",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("max_web_event_duration_sec").desc(), "cumulative_duration_sec")
)

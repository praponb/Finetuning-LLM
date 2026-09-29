from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

flagged = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "burst_id",
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("page").asc_nulls_first(), "burst_id")
)

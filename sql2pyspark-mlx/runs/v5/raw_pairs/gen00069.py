from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy("session_no")
    .agg(
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.min("event_time").alias("started_at"),
        F.percentile_approx("session_no", 0.5).alias("session_no_p50_2"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.max("event_time").alias("last_seen"),
    )
    .orderBy(F.col("session_no").desc())
)

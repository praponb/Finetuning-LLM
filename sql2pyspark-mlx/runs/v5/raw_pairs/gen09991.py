from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-07 08:00:00").cast("timestamp")) & (~F.col("page").eqNullSafe(F.lit("/search"))))
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM"))
    .withColumn("page_filled", F.coalesce("page", F.lit("UNKNOWN")))
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "page_filled",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .groupingSets(
        [["burst_id_tier", "year_start"], ["burst_id_tier"], []],
        "burst_id_tier",
        "year_start",
    )
    .agg(
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.min("event_time").alias("started_at"),
        F.grouping("burst_id_tier").alias("grouping_id"),
    )
    .orderBy(F.col("duration_sec_p95").desc())
)

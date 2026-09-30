from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy("event_time", "id")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy("session_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

cleaned = (
    df_web_events
    .filter((F.col("page").eqNullSafe(F.lit("/search"))) & F.col("event_type").isin(["search", "click"]))
    .withColumn("page_event_type", F.concat("page", F.lit(" "), "event_type"))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
)

gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

with_prev = sessionized.withColumn("prev_event_time", F.lag("event_time").over(w3))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no_2", F.sum("is_new_session").over(w4))

scored = (
    islands
    .select(
        "*",
        F.count("session_no").over(w5).alias("running_count_session_no"),
        F.sum("session_no").over(w5).alias("running_sum_session_no"),
        F.max("session_no").over(w5).alias("running_max_session_no"),
        F.min("session_no").over(w5).alias("running_min_session_no"),
    )
)

result = (
    scored
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "page_event_type",
        "page_filled",
        "session_start_flag",
        "prev_event_time",
        F.col("min_web_event_duration_sec"),
    )
)

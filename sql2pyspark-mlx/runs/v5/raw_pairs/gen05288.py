from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy(F.col("is_new_session").desc(), "id")
w5 = Window.partitionBy("page")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("page").isin(["/home", "/checkout", "/cart"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_prev = latest.withColumn("prev_event_time", F.lag("event_time").over(w2))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_session").over(w3))

with_windows = (
    islands
    .select(
        "*",
        F.row_number().over(w4).alias("seq_no"),
        F.max("duration_sec").over(w5).alias("page_max_duration_sec"),
    )
)

result = (
    with_windows
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "event_type_prefix",
        "is_new_session",
        "island_id",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("min_web_event_duration_sec").desc_nulls_first())
)

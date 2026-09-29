from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("is_new_burst", "id")
w4 = Window.partitionBy("page").orderBy("is_new_burst", "id")
w5 = Window.partitionBy("page", "event_type").orderBy(F.col("event_time").desc(), "id")
w_ordered = Window.partitionBy("event_type", "page").orderBy(F.col("seq_no").desc(), "id")
w7 = Window.partitionBy("session_id").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= F.lit("2023-05-22 00:00:00").cast("timestamp")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.concat("page", F.lit("/"), "event_type").alias("page_event_type"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

enriched = (
    islands
    .select(
        "*",
        F.ntile(10).over(w3).alias("is_new_burst_bucket"),
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w_ordered).alias("seq_no_bucket"),
        F.first("row_seq", ignorenulls=True).over(w7).alias("first_known_row_seq"),
        F.first("duration_sec", ignorenulls=True).over(w_recent).alias("first_known_duration_sec"),
        F.sum("is_new_burst_bucket").over(w_recent_2).alias("running_is_new_burst_bucket"),
    )
)

result = (
    with_windows
    .withColumn("first_known_row_seq_tier", F.when(F.col("first_known_row_seq") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["event_type", "first_known_row_seq_tier"], ["event_type"]],
        "event_type",
        "first_known_row_seq_tier",
    )
    .agg(
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
        F.round(F.avg("is_new_burst_bucket"), 2).alias("avg_is_new_burst_bucket"),
        F.round(F.sum("seq_no_bucket") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("seq_no_bucket_per_cart"),
        F.max(F.when(F.col("event_type") == "page_view", F.col("is_new_burst"))).alias("page_view_max_is_new_burst"),
    )
    .orderBy(F.col("home_rows").desc())
)

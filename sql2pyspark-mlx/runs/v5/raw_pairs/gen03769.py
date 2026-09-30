from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc_nulls_last(), "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("event_type").orderBy("burst_id", "id")
w_ordered = Window.partitionBy("event_type").orderBy("prev_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-07-06").cast("date"))
    .select(
        "*",
        F.when(F.col("event_type").isin(["add_to_cart", "search"]), "Core").otherwise("Other").alias("event_type_group"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "event_type", "event_time", "duration_sec", "resolved_page")
)

gaps = (
    deduped
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("burst_id", F.sum("is_new_session").over(w3))

with_prev = sessions_web_events.withColumn("prev_event_time", F.lag("event_time").over(w2))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w3))

with_windows = (
    islands
    .withColumn("burst_id_bucket", F.ntile(5).over(w_hist))
    .withColumn("prev_session_id", F.lag("session_id", 1).over(w2))
    .withColumn("cumulative_is_new_session", F.sum("is_new_session").over(w_ordered))
)

valid_web_events = (
    with_windows
    .filter((F.col("event_time") > F.lit("2023-06-29 17:00:00").cast("timestamp")) & (F.col("prev_event_time") >= "2024-05-28"))
    .select(
        "id",
        "event_type",
        "duration_sec",
        "resolved_page",
        "burst_id",
        "new_group_flag",
        "session_id",
        "burst_id_bucket",
        "cumulative_is_new_session",
        F.round((F.col("burst_id") * 1.15), 2).alias("burst_id_adjusted"),
    )
)

result = (
    valid_web_events
    .groupBy("event_type")
    .agg(
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec"))).alias("page_view_total_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
    )
    .filter(F.col("page_view_total_duration_sec") >= 0)
)

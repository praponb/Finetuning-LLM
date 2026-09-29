from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id", "event_type").orderBy("duration_sec", "id")
w_ordered = Window.partitionBy("page")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id")
w5 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("event_time", "id")
w7 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("page", "browser").orderBy(F.col("new_group_flag").desc_nulls_last(), "id")

base = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-05-06 07:00:00").cast("timestamp"))
    .withColumn("duration_sec_level", F.when(F.col("duration_sec") >= 384, "High").when(F.col("duration_sec") >= 100, "Medium").otherwise("Low"))
)

enriched = (
    base
    .withColumn("duration_sec_bucket", F.ntile(3).over(w))
    .withColumn("duration_sec_bucket_2", F.ntile(10).over(w2))
    .withColumn("page_count_duration_sec", F.count("duration_sec").over(w_ordered))
)

prev_events = enriched.withColumn("prev_event_time", F.lag("event_time").over(w4))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("new_group_flag").over(w5))

prev_events_2 = sessionized.withColumn("prev_event_time_2", F.lag("event_time").over(w6))

gaps_2 = (
    prev_events_2
    .withColumn("session_start_flag", F.when(F.col("prev_event_time_2").isNull() | (F.col("event_time") > F.col("prev_event_time_2") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps_2.withColumn("session_no", F.sum("session_start_flag").over(w7))

joined = (
    islands
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.started_at") < "2023-10-01")
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.duration_sec_bucket",
        "a.duration_sec_bucket_2",
        "a.page_count_duration_sec",
        "a.new_group_flag",
        "a.prev_event_time_2",
        F.col("s.browser"),
    )
)

joined_web_events = (
    joined
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.new_group_flag",
        "src.browser",
        F.col("b.browser").alias("session_browser"),
    )
)

deduped = (
    joined_web_events
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

grouped = (
    deduped
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .rollup("page", "new_group_flag_tier")
    .agg(
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.avg("new_group_flag").alias("avg_new_group_flag"),
        F.sort_array(F.collect_set("session_browser")).alias("session_browser_list"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.grouping("page").alias("grouping_level"),
    )
    .filter(F.col("avg_new_group_flag") > 0)
)

result = (
    grouped
    .select(
        "page",
        "new_group_flag_tier",
        "avg_new_group_flag",
        "session_browser_list",
        "page_list",
        "grouping_level",
    )
    .orderBy(F.col("grouping_level").asc_nulls_first(), "new_group_flag_tier")
)

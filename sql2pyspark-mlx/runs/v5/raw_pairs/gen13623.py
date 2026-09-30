from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page", "event_type").orderBy("prev_event_time")
w4 = Window.partitionBy("page").orderBy(F.col("dedup_rank").desc(), "id")

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") < "2025-09-01"))
    .select("*", F.substring("page", 1, 3).alias("page_prefix"))
)

prev_events = filtered.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

deduped = (
    sessions_web_events
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.browser"), F.col("s.device"), F.col("s.country"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.customer_id") == 10)
    .select(
        "e.*",
        F.col("s.browser").alias("session_browser"),
        F.col("s.country").alias("session_country"),
    )
)

deduped_web_events = (
    with_dims
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

totals = (
    deduped_web_events
    .groupBy(F.date_trunc("WEEK", "prev_event_time").alias("week_start"))
    .agg(
        F.max(F.when(F.col("event_type") == "page_view", F.col("new_group_flag"))).alias("page_view_max_new_group_flag"),
        F.avg(F.when(F.col("page") == "/home", F.col("new_group_flag"))).alias("home_avg_new_group_flag"),
        F.size(F.collect_set("session_country")).alias("num_session_countries"),
    )
)

result = (
    totals
    .select("week_start", "page_view_max_new_group_flag")
    .orderBy(F.col("page_view_max_new_group_flag").asc_nulls_last())
)

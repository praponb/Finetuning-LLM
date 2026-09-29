from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")
w2 = Window.partitionBy("page")
w3 = Window.partitionBy("session_id").orderBy(F.col("next_duration_sec").desc_nulls_first(), "id")
w4 = Window.partitionBy("device", "session_id").orderBy("next_duration_sec", "id")
w5 = Window.partitionBy("page").orderBy("event_time", "id")
w6 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy("next_page_count_is_click", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("browser").orderBy("is_new_session", "id")

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_type") == "page_view"))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type") == "click", 1).otherwise(0).alias("is_click"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.lead("duration_sec", 2).over(w).alias("next_duration_sec"),
        F.count("is_click").over(w2).alias("page_count_is_click"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.country"), F.col("p.browser"), F.col("p.device"))
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    deduped
    .select(
        "*",
        F.lead("page_count_is_click", 1).over(w4).alias("next_page_count_is_click"),
        F.max("duration_sec").over(w2).alias("page_max_duration_sec"),
    )
)

with_gaps = (
    scored
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w5) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id_2", F.sum("is_new_session").over(w6))

enriched = (
    sessionized
    .filter(F.col("next_page_count_is_click") >= 11)
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "is_click",
        "browser",
        "page_max_duration_sec",
        "is_new_session",
        "session_id_2",
        F.avg("duration_sec").over(w7).alias("running_avg_duration_sec"),
        F.count("duration_sec").over(w7).alias("running_count_duration_sec"),
        F.sum("duration_sec").over(w7).alias("running_sum_duration_sec"),
        F.lead("session_id_2", 1).over(w8).alias("next_session_id_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "is_click",
        "browser",
        "page_max_duration_sec",
        "is_new_session",
        "session_id_2",
        F.when(F.col("browser").rlike("^(Firefox|Chrome)$"), "OTHER_ZONE").when(F.col("browser").rlike("^(Safari|Edge)$"), "PRIORITY").otherwise("GROUP_B").alias("browser_zone"),
    )
    .orderBy(F.col("session_id").desc())
)

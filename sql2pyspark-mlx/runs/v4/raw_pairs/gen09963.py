from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("island_id_tier").orderBy(F.col("click_session_start_flag").desc())

ranked = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.browser"), F.col("s.country"), F.col("s.device"))
)

with_prev = enriched.withColumn("prev_event_time", F.lag("event_time").over(w2))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("island_id", F.sum("session_start_flag").over(w3))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("island_id") >= 8, "HIGH").otherwise("LOW").alias("island_id_tier"),
        "browser",
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sum(F.when(F.col("event_type") == "click", F.col("session_start_flag")).otherwise(0)).alias("click_session_start_flag"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
    )
    .withColumn("click_session_start_flag_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("island_id_tier").desc_nulls_first())
)

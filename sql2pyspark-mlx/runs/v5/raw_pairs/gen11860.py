from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w4 = Window.partitionBy("quarter_start").orderBy(F.col("avg_session_id").desc())

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-11 20:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "page",
        "event_time",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
        F.round((F.col("duration_sec") * 1.07), 2).alias("duration_sec_adjusted"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.first("page", ignorenulls=True).over(w).alias("first_known_page"),
        F.avg("duration_sec_adjusted").over(w2).alias("duration_sec_adjusted_avg_2"),
    )
)

flagged = (
    with_windows
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec_adjusted",
        "page_filled",
        "first_known_page",
        F.sum("is_new_session").over(w).alias("session_id"),
    )
)

with_prev = islands.withColumn("prev_event_time", F.lag("event_time").over(w3))

flagged_2 = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands_web_events = flagged_2.withColumn("session_id_2", F.sum("session_start_flag").over(w))

result = (
    islands_web_events
    .groupBy(
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        "page",
        F.date_trunc("YEAR", "prev_event_time").alias("year_start"),
    )
    .agg(
        F.percentile_approx("session_start_flag", 0.95).alias("session_start_flag_p95"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .withColumn("avg_session_id_rank", F.rank().over(w4))
    .orderBy(F.col("avg_session_id").desc_nulls_first())
)

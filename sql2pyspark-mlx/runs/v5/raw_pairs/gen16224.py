from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy(F.col("running_max_duration_sec_rounded").desc_nulls_first())

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

scored = (
    base
    .select(
        "*",
        F.max("duration_sec_rounded").over(w).alias("running_max_duration_sec_rounded"),
        F.count("duration_sec_rounded").over(w).alias("running_count_duration_sec_rounded"),
        F.min("duration_sec_rounded").over(w).alias("running_min_duration_sec_rounded"),
    )
)

with_gaps = (
    scored
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w2) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w3))

filtered = (
    islands
    .filter((F.col("event_time") > F.lit("2023-05-24 17:00:00").cast("timestamp")) & (F.col("duration_sec_rounded") <= 77))
    .select(
        "*",
        F.substring("event_type", 1, 3).alias("event_type_prefix"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page"),
    )
)

with_prev = filtered.withColumn("prev_event_time", F.lag("event_time").over(w2))

with_gaps_2 = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps_2
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_rounded",
        "page_clean",
        "running_max_duration_sec_rounded",
        "running_count_duration_sec_rounded",
        "running_min_duration_sec_rounded",
        "new_group_flag",
        "island_id",
        F.sum("session_start_flag").over(w3).alias("burst_id"),
    )
)

enriched = (
    sessionized
    .select(
        "id",
        "event_type",
        "duration_sec_rounded",
        "running_min_duration_sec_rounded",
        "burst_id",
        F.sum("island_id").over(w4).alias("last7_sum_island_id"),
        F.min("island_id").over(w4).alias("last7_min_island_id"),
        F.avg("island_id").over(w4).alias("last7_avg_island_id"),
        F.max("island_id").over(w4).alias("last7_max_island_id"),
        F.rank().over(w5).alias("running_max_duration_sec_rounded_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("running_max_duration_sec_rounded_rank") >= 7)
    .select(
        "id",
        "event_type",
        "duration_sec_rounded",
        "running_min_duration_sec_rounded",
        "burst_id",
        "last7_avg_island_id",
        "last7_max_island_id",
        F.coalesce("last7_sum_island_id", F.lit(0)).alias("last7_sum_island_id_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "event_type",
        "running_min_duration_sec_rounded",
        "burst_id",
        "last7_avg_island_id",
    )
)

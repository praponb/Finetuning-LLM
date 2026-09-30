from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("burst_id")
w4 = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")
w5 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("page", "session_id").orderBy("burst_id", "id")
w7 = Window.partitionBy("event_type").orderBy("event_time", "id")
w8 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w10 = Window.orderBy(F.col("burst_id_rank").desc_nulls_last())

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

enriched = (
    islands
    .filter(F.col("burst_id") >= 4)
    .select(
        "id",
        "event_type",
        "event_time",
        "is_new_burst",
        "burst_id",
        F.rank().over(w3).alias("burst_id_rank"),
        F.lag("is_new_burst", 2, 0).over(w4).alias("prev_is_new_burst"),
        F.count("duration_sec").over(w5).alias("duration_sec_count_7d"),
        F.lead("duration_sec", 1).over(w6).alias("next_duration_sec"),
    )
)

flagged = (
    enriched
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w7) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "event_type",
        "event_time",
        "burst_id",
        "burst_id_rank",
        "next_duration_sec",
        F.sum("session_start_flag").over(w8).alias("burst_id_2"),
    )
)

remaining_keys = (
    sessionized
    .select("id", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") < 120)
        .select("id", "event_type")
    )
)

kept_rows = (
    sessionized
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.avg("burst_id").over(w9).alias("burst_id_avg_7d"),
        F.percent_rank().over(w10).alias("burst_id_rank_percent_rank"),
        F.count("burst_id_2").over(w9).alias("burst_id_2_count_7d"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("burst_id_2") >= 9, "HIGH").otherwise("LOW").alias("burst_id_2_tier"),
    )
    .agg(
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.count_if(F.col("event_type") == "add_to_cart").alias("add_to_cart_events"),
        F.percentile_approx("burst_id_rank_percent_rank", 0.95).alias("burst_id_rank_percent_rank_p95"),
        F.percentile_approx("burst_id_2_count_7d", 0.5).alias("burst_id_2_count_7d_p50"),
    )
)

result = (
    summary
    .select(
        "burst_id_2_tier",
        "burst_id_quartiles",
        "add_to_cart_events",
        "burst_id_2_count_7d_p50",
    )
    .orderBy(F.col("burst_id_2_count_7d_p50").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("last4_sum_duration_sec").desc(), "id")
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").asc_nulls_last())
w4 = Window.partitionBy("session_id")
w5 = Window.partitionBy("session_id", "event_type").orderBy("duration_sec_dense_rank", "id")
w6 = Window.partitionBy("event_type")

with_windows = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.min("duration_sec").over(w_recent).alias("last4_min_duration_sec"),
        F.count("duration_sec").over(w_recent).alias("last4_count_duration_sec"),
        F.sum("duration_sec").over(w_recent).alias("last4_sum_duration_sec"),
    )
)

metrics = (
    with_windows
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "last4_min_duration_sec",
        "last4_count_duration_sec",
        "last4_sum_duration_sec",
        F.ntile(10).over(w2).alias("last4_sum_duration_sec_bucket"),
        F.dense_rank().over(w3).alias("duration_sec_dense_rank"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "session_id",
        "event_type",
        "last4_min_duration_sec",
        "duration_sec_dense_rank",
        F.round((F.col("duration_sec_dense_rank") / F.sum("duration_sec_dense_rank").over(w4)), 4).alias("duration_sec_dense_rank_share"),
        F.lead("last4_sum_duration_sec_bucket", 1).over(w5).alias("next_last4_sum_duration_sec_bucket"),
        F.avg("last4_min_duration_sec").over(w6).alias("event_type_avg_last4_min_duration_sec"),
    )
)

cleaned = (
    enriched
    .filter(F.col("last4_min_duration_sec") > 230)
    .select(
        "id",
        "last4_min_duration_sec",
        "duration_sec_dense_rank",
        "duration_sec_dense_rank_share",
        "next_last4_sum_duration_sec_bucket",
        F.upper("event_type").alias("event_type_upper"),
    )
)

result = (
    cleaned
    .cube(
        F.when(F.col("last4_min_duration_sec") >= 510, "LARGE").otherwise("SMALL").alias("last4_min_duration_sec_tier"),
        F.when(F.col("duration_sec_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("duration_sec_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("duration_sec_dense_rank"), 2).alias("avg_duration_sec_dense_rank"),
        F.percentile_approx("last4_min_duration_sec", [0.25, 0.5, 0.75]).alias("last4_min_duration_sec_quartiles"),
    )
)

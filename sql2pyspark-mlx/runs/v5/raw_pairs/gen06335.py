from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy(F.col("discount_pct").desc())
w_running = Window.partitionBy("channel").orderBy("is_bundle", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id")
w5 = Window.orderBy("cumulative_discount_pct_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("event_seq", "id")
w7 = Window.orderBy("event_seq", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("promo_code") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "ends_on",
        "discount_pct",
        F.sum("discount_pct").over(w).alias("cumulative_discount_pct"),
        F.rank().over(w_ordered).alias("discount_pct_rank"),
        F.sum("is_bundle").over(w_running).alias("cumulative_is_bundle"),
        F.row_number().over(w4).alias("event_seq"),
        F.sum("discount_pct").over(w_running).alias("cumulative_discount_pct_2"),
    )
)

filtered = (
    metrics
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-10-20")))
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "discount_pct_rank",
        "cumulative_is_bundle",
        "event_seq",
        "cumulative_discount_pct_2",
        F.round((F.col("cumulative_is_bundle") / F.col("discount_pct_rank")), 2).alias("cumulative_is_bundle_per_discount_pct_rank"),
        F.coalesce("event_seq", F.lit(0)).alias("event_seq_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "cumulative_is_bundle",
        "event_seq",
        "cumulative_is_bundle_per_discount_pct_rank",
        "event_seq_filled",
        F.sum("cumulative_is_bundle_per_discount_pct_rank").over(w5).alias("cumulative_cumulative_is_bundle_per_discount_pct_rank"),
        F.ntile(3).over(w6).alias("event_seq_bucket"),
        F.avg("cumulative_is_bundle_per_discount_pct_rank").over(w7).alias("cumulative_is_bundle_per_discount_pct_rank_avg_6"),
    )
)

valid = (
    with_windows
    .filter(F.col("ends_on") > F.lit("2023-11-01").cast("date"))
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "cumulative_is_bundle",
        "event_seq",
        "event_seq_filled",
        "cumulative_cumulative_is_bundle_per_discount_pct_rank",
        "cumulative_is_bundle_per_discount_pct_rank_avg_6",
        F.col("cumulative_is_bundle_per_discount_pct_rank_avg_6").cast("string").alias("cumulative_is_bundle_per_discount_pct_rank_avg_6_num"),
    )
)

result = (
    valid
    .select("id", "event_seq_filled", "discount_pct")
    .unpivot(
        ids=["id"],
        values=["event_seq_filled", "discount_pct"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

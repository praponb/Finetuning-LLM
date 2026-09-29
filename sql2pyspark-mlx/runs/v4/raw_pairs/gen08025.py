from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("days_since_starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id")
w3 = Window.partitionBy("channel").orderBy(F.col("days_since_starts_on").desc_nulls_first())
w4 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-08-22").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.date_format("starts_on", "yyyy-MM").alias("starts_on_label"))
)

base = (
    filtered
    .filter(F.col("promo_code") != "SPRING10")
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
    )
)

scored = (
    base
    .select(
        "*",
        F.sum("days_since_starts_on").over(w).alias("running_days_since_starts_on"),
        F.ntile(4).over(w2).alias("discount_pct_bucket"),
        F.dense_rank().over(w3).alias("days_since_starts_on_dense_rank"),
        F.first("channel", ignorenulls=True).over(w4).alias("first_known_channel"),
        F.sum("discount_pct").over(w5).alias("cumulative_discount_pct"),
    )
)

stats = (
    scored
    .groupBy(
        F.when(F.col("days_since_starts_on_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("days_since_starts_on_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.percentile_approx("days_since_starts_on_dense_rank", 0.95).alias("days_since_starts_on_dense_rank_p95"),
        F.max(F.when(F.col("channel") == "web", F.col("discount_pct"))).alias("web_max_discount_pct"),
    )
)

result = (
    stats
    .select(
        "days_since_starts_on_dense_rank_tier",
        "store_count",
        "days_since_starts_on_dense_rank_p95",
        "web_max_discount_pct",
    )
)

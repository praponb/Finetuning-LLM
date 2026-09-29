from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code")
w3 = Window.orderBy(F.col("discount_pct").desc(), "id")

valid = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-09-16").cast("date")) & (F.col("promo_code") == "BUNDLE"))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_format("ends_on", "yyyy-MM").alias("ends_on_label"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "ends_on_label",
        "channel_clean",
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.sum("discount_pct").over(w2).alias("promo_code_sum_discount_pct"),
        F.ntile(5).over(w3).alias("discount_pct_bucket"),
    )
)

base = (
    metrics
    .filter((F.col("ends_on") >= F.lit("2023-07-03").cast("date")) & (F.col("ends_on") < "2024-02-27"))
    .select(
        "*",
        F.coalesce("discount_pct_bucket", F.lit(0)).alias("discount_pct_bucket_filled"),
    )
)

grouped = (
    base
    .groupBy(
        F.date_trunc("MONTH", "ends_on").alias("activity_month"),
        F.when(F.col("discount_pct_bucket_filled") >= 89, "HIGH").otherwise("LOW").alias("discount_pct_bucket_filled_tier"),
    )
    .agg(
        F.percentile_approx("discount_pct_bucket_filled", [0.25, 0.5, 0.75]).alias("discount_pct_bucket_filled_quartiles"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct_2"),
        F.max(F.when(F.col("promo_code") == "BUNDLE", F.col("promo_code_sum_discount_pct"))).alias("bundle_max_promo_code_sum_discount_pct"),
    )
    .filter(F.col("avg_discount_pct_2") >= 0)
)

result = (
    grouped
    .select("activity_month", "discount_pct_bucket_filled_tier", "avg_discount_pct")
)

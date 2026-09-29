from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("promo_code")
w_running = Window.orderBy("discount_pct", "id").rowsBetween(-5, Window.currentRow)
w_part = Window.orderBy("cumulative_discount_pct", "id")

base = (
    df_promotions
    .filter(F.col("ends_on") <= "2024-10-05")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.upper("channel").alias("channel_upper"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "channel", "promo_code", "ends_on", "discount_pct", "channel_upper")
)

metrics = (
    ranked
    .filter(F.col("ends_on").between("2023-12-01", "2025-08-01"))
    .select(
        "id",
        "discount_pct",
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_2_prior"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4).alias("discount_pct_share"),
    )
)

enriched = (
    metrics
    .withColumn("discount_pct_avg_2_prior_avg_5", F.avg("discount_pct_avg_2_prior").over(w_running))
    .withColumn("cumulative_discount_pct_bucket", F.ntile(4).over(w_part))
)

base_promotions = (
    enriched
    .filter(F.col("cumulative_discount_pct") < 1)
    .select(
        "*",
        (F.col("discount_pct") * F.col("discount_pct_avg_2_prior")).alias("discount_pct_times_discount_pct_avg_2_prior"),
    )
)

result = (
    base_promotions
    .groupBy(
        F.when(F.col("cumulative_discount_pct_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("cumulative_discount_pct_bucket_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_discount_pct_bucket", 0.9).alias("cumulative_discount_pct_bucket_p90"),
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
    )
    .filter(F.col("cumulative_discount_pct_bucket_p90") >= 0)
    .orderBy("row_count_2")
)

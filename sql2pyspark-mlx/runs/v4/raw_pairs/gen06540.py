from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel").orderBy("discount_pct")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel", "promo_code")
w4 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-08-28").cast("date")) & (F.col("channel") == "mobile"))
    .withColumn("promo_code_title", F.initcap(F.trim("promo_code")))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("NONE")))
)

with_windows = (
    base
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("discount_pct_cume_dist"),
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct"),
        F.stddev_samp("discount_pct").over(w3).alias("channel_stddev_samp_discount_pct"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_2"),
    )
)

base_promotions = (
    with_windows
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_code_title",
        "channel_filled",
        "discount_pct_cume_dist",
        "channel_stddev_samp_discount_pct",
        "discount_pct_avg_2",
        (F.col("channel_stddev_samp_discount_pct") + F.col("cumulative_discount_pct")).alias("channel_stddev_samp_discount_pct_plus_cumulative_discount_pct"),
        (F.col("discount_pct") / F.nullif("discount_pct_cume_dist", F.lit(0))).alias("discount_pct_per_discount_pct_cume_dist"),
    )
)

summary = (
    base_promotions
    .groupBy(
        F.when(F.col("discount_pct_per_discount_pct_cume_dist") >= 75, "PREMIUM").otherwise("STANDARD").alias("discount_pct_per_discount_pct_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("discount_pct_per_discount_pct_cume_dist", [0.25, 0.5, 0.75]).alias("discount_pct_per_discount_pct_cume_dist_quartiles"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)

result = (
    summary
    .select(
        "discount_pct_per_discount_pct_cume_dist_tier",
        "distinct_ids",
        "distinct_ids_2",
    )
)

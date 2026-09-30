from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

enriched = (
    lines
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
)

valid = (
    enriched
    .filter((F.col("effective_from") >= F.lit("2024-06-16").cast("date")) & (F.col("val_cume_dist") > 0.69))
    .withColumn("effective_from_label", F.date_format("effective_from", "MMM yyyy"))
)

filtered = (
    valid
    .filter(F.col("effective_from") >= F.lit("2024-03-24").cast("date"))
    .select(
        "*",
        F.concat("plan", F.lit(", "), "currency").alias("plan_currency"),
        F.substring("plan", 1, 4).alias("plan_prefix"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("val") >= 68, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
        "currency",
    )
    .agg(
        F.percentile_approx("val_percent_rank", 0.5).alias("val_percent_rank_p50"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.round(F.sum("val_cume_dist") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("val_cume_dist_per_pro"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("val_cume_dist"))).alias("eur_avg_val_cume_dist"),
    )
    .orderBy("eur_avg_val_cume_dist")
)

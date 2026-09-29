from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        F.when(F.col("discount_pct") >= 0.34, "High").when(F.col("discount_pct") >= 0.18, "Medium").otherwise("Low").alias("discount_pct_tier"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("discount_pct") >= 0.34, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles_2"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles_3"),
    )
)

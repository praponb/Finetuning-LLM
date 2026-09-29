from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.weekofyear("ends_on").alias("ends_on_weekofyear"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("discount_pct") >= 0.36, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
        "promo_code",
        F.date_trunc("quarter", "starts_on").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
        F.count_if(F.col("promo_code") == "SPRING10").alias("spring10_events"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.count("*").alias("cnt"),
        F.round(F.avg("ends_on_weekofyear"), 2).alias("avg_ends_on_weekofyear"),
        F.sum(F.when(F.col("channel") == "web", F.col("ends_on_weekofyear")).otherwise(0)).alias("web_ends_on_weekofyear"),
    )
    .orderBy(F.col("avg_ends_on_weekofyear").desc())
)

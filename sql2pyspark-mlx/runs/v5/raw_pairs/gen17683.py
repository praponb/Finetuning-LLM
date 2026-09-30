from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    flattened
    .groupBy(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
        F.when(F.col("tier_value") >= 70, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.avg(F.when(F.col("plan") == "enterprise", F.col("tier_value"))).alias("enterprise_avg_tier_value"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("enterprise_avg_tier_value").desc_nulls_first())
)

from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from"))
    .cube("week_start")
    .agg(
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles_2"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("tier_value_per_eur"),
        F.grouping("week_start").alias("grouping_level"),
    )
    .orderBy("tier_value_per_eur")
)

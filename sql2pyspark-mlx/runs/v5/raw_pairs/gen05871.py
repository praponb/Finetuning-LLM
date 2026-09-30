from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    exploded
    .groupBy(
        "currency",
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.avg("tier_value").alias("avg_tier_value"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value_2"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.count("*").alias("row_count"),
    )
)

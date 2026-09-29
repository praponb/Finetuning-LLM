from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    flattened
    .groupBy("plan")
    .agg(
        F.percentile_approx("tier_value", 0.95).alias("tier_value_p95"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
    )
    .filter(F.col("eur_rows") > 1)
    .orderBy(F.col("tier_value_p95").asc_nulls_last())
)

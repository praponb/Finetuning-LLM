from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

result = (
    lines
    .groupBy("currency")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("currency") == "USD", F.col("val")).otherwise(0)).alias("usd_val"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
    )
    .orderBy(F.col("eur_count").desc())
)

from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("element"))

result = (
    flattened
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
    .orderBy(F.col("currency").desc())
)

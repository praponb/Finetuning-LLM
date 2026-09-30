from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "effective_from", F.explode("tier_prices").alias("val"))

result = (
    lines
    .groupBy(F.when(F.col("val") >= 88, "HIGH").otherwise("LOW").alias("val_tier"))
    .agg(
        F.percentile_approx("val", 0.95).alias("val_p95"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.max("val").alias("max_val"),
    )
)

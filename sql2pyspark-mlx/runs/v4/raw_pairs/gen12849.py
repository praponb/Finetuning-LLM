from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", F.explode("tier_prices").alias("val"))

filtered = (
    flattened
    .filter(F.col("val").between(20, 32))
    .select(
        "*",
        F.coalesce("val", F.lit(0)).alias("val_filled"),
        F.round((F.col("val") * 0.9), 2).alias("adjusted_val"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") >= "2025-09-28")
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("adjusted_val").between(28, 54))
    .withColumn("least_val_adjusted_val", F.least("val", "adjusted_val"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("least_val_adjusted_val") >= 64, "HIGH").otherwise("LOW").alias("least_val_adjusted_val_tier"),
        F.when(F.col("val_filled") >= 69, "LARGE").otherwise("SMALL").alias("val_filled_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.round(F.avg("val_filled"), 2).alias("avg_val_filled"),
    )
    .orderBy(F.col("distinct_ids").desc())
)

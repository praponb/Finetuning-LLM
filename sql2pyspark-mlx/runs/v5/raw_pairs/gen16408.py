from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", F.explode("tier_limits").alias("val"))

result = (
    flattened
    .groupBy(F.when(F.col("val") >= 67, "LARGE").otherwise("SMALL").alias("val_tier"))
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.percentile_approx("val", 0.95).alias("val_p95"),
    )
    .orderBy(F.col("val_tier").desc_nulls_last())
)

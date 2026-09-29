from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode_outer("discounts").alias("val"))

result = (
    exploded
    .groupBy(F.when(F.col("val") >= 79, "HIGH").otherwise("LOW").alias("val_tier"))
    .agg(
        F.min("val").alias("min_val"),
        F.min("val").alias("min_val_2"),
        F.sum("val").alias("total_val"),
        F.percentile_approx("val", 0.5).alias("val_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("val", 0.95).alias("val_p95"),
    )
    .orderBy(F.col("min_val").desc())
)

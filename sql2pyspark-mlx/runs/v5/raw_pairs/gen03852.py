from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

result = (
    exploded
    .withColumn("val_tier", F.when(F.col("val") >= 81, "LARGE").otherwise("SMALL"))
    .groupingSets([["plan", "val_tier"], ["plan"]], "plan", "val_tier")
    .agg(
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("val_per_starter"),
        F.max("val").alias("max_val"),
    )
    .orderBy(F.col("val_per_starter").desc_nulls_last())
)

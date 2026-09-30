from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("element"))

result = (
    exploded
    .groupBy(
        F.when(F.col("element") >= 80, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.percentile_approx("element", 0.9).alias("element_p90"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles_2"),
    )
    .orderBy(F.col("element_tier").asc_nulls_last())
)

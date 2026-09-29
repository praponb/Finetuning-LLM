from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    flattened
    .withColumn("element_tier", F.when(F.col("element") >= 66, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["currency", "element_tier"], ["currency"]],
        "currency",
        "element_tier",
    )
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles_2"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.count("*").alias("event_count"),
    )
)

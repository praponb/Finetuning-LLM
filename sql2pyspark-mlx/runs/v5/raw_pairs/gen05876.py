from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

result = (
    exploded
    .rollup(
        "plan",
        F.when(F.col("element") >= 83, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.min("element").alias("min_element"),
        F.percentile_approx("element", 0.95).alias("element_p95"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("element_tier").asc_nulls_first())
)

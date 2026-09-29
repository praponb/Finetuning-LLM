from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 75, "LARGE").otherwise("SMALL").alias("element_tier"),
        "plan",
    )
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("element_per_starter"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.percentile_approx("element", 0.9).alias("element_p90"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
    )
    .filter(F.col("avg_element") < 0)
    .orderBy(F.col("avg_element").desc())
)

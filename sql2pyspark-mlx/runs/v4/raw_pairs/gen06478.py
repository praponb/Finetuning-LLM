from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("element"),
    )
)

base = (
    exploded
    .filter(F.col("effective_from").between("2023-06-01", "2024-06-13"))
    .withColumn("effective_from_month_end", F.last_day("effective_from"))
    .withColumn("plan_prefix", F.substring("plan", 1, 4))
)

filtered = (
    base
    .filter(F.col("element") == 27)
    .select(
        "*",
        F.coalesce("element", F.lit(0)).alias("element_filled"),
        F.add_months("effective_from", 3).alias("effective_from_plus_months"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("element") >= 79, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.min("element").alias("min_element"),
        F.percentile_approx("element_filled", [0.25, 0.5, 0.75]).alias("element_filled_quartiles"),
    )
    .orderBy(F.col("num_plans").desc())
)

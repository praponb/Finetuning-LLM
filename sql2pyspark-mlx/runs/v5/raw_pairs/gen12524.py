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

result = (
    exploded
    .cube("plan")
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.min("element").alias("min_element"),
        F.count("*").alias("num_rows"),
        F.grouping("plan").alias("grouping_level"),
    )
)

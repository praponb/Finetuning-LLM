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
    .groupBy(
        F.when(F.col("element") >= 70, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.avg("element"), 2).alias("avg_element"),
    )
)

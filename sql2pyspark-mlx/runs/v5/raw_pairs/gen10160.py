from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    lines
    .withColumn("quarter_start", F.date_trunc("quarter", "effective_from"))
    .cube("quarter_start", "currency")
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.grouping_id().alias("grouping_id"),
    )
)

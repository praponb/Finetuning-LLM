from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("element"))
)

result = (
    flattened
    .groupBy(
        F.date_trunc("quarter", "effective_from").alias("quarter_start"),
        F.date_trunc("year", "effective_from").alias("year_start"),
    )
    .agg(
        F.percentile_approx("element", 0.9).alias("element_p90"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("element"), 2).alias("avg_element"),
    )
    .filter(F.col("num_rows") > 1)
)

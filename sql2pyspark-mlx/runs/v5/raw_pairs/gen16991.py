from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .cube("year_start")
    .agg(
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.sum(F.when(F.col("currency") == "USD", F.col("tier_value")).otherwise(0)).alias("usd_tier_value"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("usd_tier_value").asc_nulls_last())
)

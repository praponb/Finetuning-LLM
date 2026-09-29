from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_prices").alias("tier_value"))
)

result = (
    flattened
    .groupBy("currency")
    .agg(
        F.max(F.when(F.col("currency") == "THB", F.col("tier_value"))).alias("thb_max_tier_value"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
    )
)

from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    exploded
    .rollup("currency")
    .agg(
        F.avg(F.when(F.col("currency") == "EUR", F.col("tier_value"))).alias("eur_avg_tier_value"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
    .filter(F.col("eur_avg_tier_value") > 0)
    .orderBy(F.col("currency").desc_nulls_last())
)

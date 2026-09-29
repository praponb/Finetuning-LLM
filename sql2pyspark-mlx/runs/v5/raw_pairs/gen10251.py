from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("element"))
)

result = (
    flattened
    .groupBy("currency")
    .agg(
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("element_per_starter"),
        F.min("element").alias("min_element"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.max("element").alias("max_element"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
    )
    .filter(F.col("thb_rows") >= 1)
    .orderBy("max_element")
)

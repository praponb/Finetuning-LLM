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
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.min("element").alias("min_element"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("thb_rows") < 1)
    .orderBy(F.col("currency").desc_nulls_first())
)

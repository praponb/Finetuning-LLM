from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 62, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("element_per_basic"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("element_per_thb"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum(F.when(F.col("currency") == "THB", F.col("element")).otherwise(0)).alias("thb_element"),
    )
    .filter(F.col("distinct_currencies") < 1000)
    .orderBy(F.col("thb_element").desc_nulls_last())
)

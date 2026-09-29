from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.round(F.avg("element"), 2).alias("avg_element_2"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("element_per_pro"),
        F.max("element").alias("max_element"),
        F.sum(F.when(F.col("plan") == "starter", F.col("element")).otherwise(0)).alias("starter_element"),
    )
    .filter(F.col("element_per_pro") > 0)
)

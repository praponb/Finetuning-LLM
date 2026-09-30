from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("tier_value") >= 88, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
        "plan",
    )
    .agg(
        F.avg(F.when(F.col("plan") == "basic", F.col("tier_value"))).alias("basic_avg_tier_value"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
    )
    .filter(F.col("thb_rows") < 1000)
)

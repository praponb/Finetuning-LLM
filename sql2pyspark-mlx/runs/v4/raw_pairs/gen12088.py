from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("tier_value"))
)

base = (
    lines
    .filter(F.col("tier_value") >= 78)
    .withColumn("tier_value_int", F.col("tier_value").cast("int"))
)

filtered = (
    base
    .filter(~F.col("currency").isin(["EUR", "THB"]))
    .select(
        "*",
        (F.col("tier_value") / F.nullif("tier_value_int", F.lit(0))).alias("tier_value_per_tier_value_int"),
    )
)

result = (
    filtered
    .select(
        "currency",
        F.col("tier_value").alias("TIER_VALUE_VAL"),
        F.col("tier_value_per_tier_value_int").alias("TIER_VALUE_PER_TIER_VALUE_INT"),
    )
    .unpivot(
        ["currency"],
        ["TIER_VALUE_VAL", "TIER_VALUE_PER_TIER_VALUE_INT"],
        "metric_name",
        "metric_value",
    )
)

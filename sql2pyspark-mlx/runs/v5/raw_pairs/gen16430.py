from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("tier_value_times_tier_value_filled").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("tier_value").isNotNull())
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        F.concat("plan", F.lit("/"), "currency").alias("plan_currency"),
    )
)

valid = (
    cleaned
    .filter(F.col("effective_from") > "2025-09-01")
    .select("*", F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"))
)

valid_price_tiers = (
    valid
    .filter((F.col("effective_from") >= F.lit("2024-05-25").cast("date")) & (F.col("effective_from") < "2025-05-01"))
    .select(
        "*",
        (F.col("tier_value") * F.col("tier_value_filled")).alias("tier_value_times_tier_value_filled"),
    )
)

result = (
    valid_price_tiers
    .filter(F.col("tier_value_times_tier_value_filled") >= 29)
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        "tier_value_filled",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.stddev_samp("tier_value_times_tier_value_filled").over(w2).alias("plan_stddev_samp_tier_value_times_tier_value_filled"),
        F.dense_rank().over(w3).alias("tier_value_times_tier_value_filled_dense_rank"),
    )
)

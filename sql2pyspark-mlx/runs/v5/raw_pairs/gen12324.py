from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy("tier_value")
w4 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.max("tier_value").over(w).alias("plan_max_tier_value"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.count("tier_value").over(w).alias("plan_count_tier_value"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist_2"),
        F.count("tier_value").over(w4).alias("currency_count_tier_value"),
    )
)

filtered = (
    scored
    .filter(F.col("currency_count_tier_value") <= 12)
    .select(
        "*",
        F.when(F.col("currency_count_tier_value") >= 13, "High").when(F.col("currency_count_tier_value") >= 7, "Medium").otherwise("Low").alias("currency_count_tier_value_band"),
        (F.col("tier_value_cume_dist_2") + F.col("currency_count_tier_value")).alias("tier_value_cume_dist_2_plus_currency_count_tier_value"),
    )
)

valid = (
    filtered
    .filter((F.col("effective_from") > F.lit("2024-04-30").cast("date")) & (F.col("plan") == "pro"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "currency_count_tier_value",
        F.least("plan_count_tier_value", "tier_value_cume_dist_2_plus_currency_count_tier_value").alias("least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value"),
    )
)

base = (
    valid
    .filter(F.col("effective_from") >= F.lit("2024-03-08").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value_cume_dist_2",
        "currency_count_tier_value",
        "least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value",
        (F.col("tier_value_cume_dist_2") * F.col("currency_count_tier_value")).alias("tier_value_cume_dist_2_times_currency_count_tier_value"),
        F.round("least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value", 0).alias("least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value_rounded"),
    )
)

result = (
    base
    .select(
        "id",
        "plan",
        "currency",
        "tier_value_cume_dist_2",
        "currency_count_tier_value",
        "least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value",
        "tier_value_cume_dist_2_times_currency_count_tier_value",
    )
    .orderBy(F.col("currency").desc_nulls_last(), F.col("plan").desc())
)

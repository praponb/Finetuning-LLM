from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

base = (
    exploded
    .filter(F.col("tier_value").between(25, 72))
    .select("*", F.last_day("effective_from").alias("effective_from_month_end"))
)

with_windows = (
    base
    .select(
        "*",
        F.max("tier_value").over(w).alias("plan_max_tier_value"),
        F.count("tier_value").over(w2).alias("currency_count_tier_value"),
    )
)

valid = (
    with_windows
    .filter(F.col("tier_value") > 23)
    .select(
        "effective_from",
        "currency",
        "tier_value",
        "effective_from_month_end",
        "currency_count_tier_value",
        (F.col("plan_max_tier_value") * F.col("tier_value")).alias("plan_max_tier_value_times_tier_value"),
    )
)

with_windows_price_tiers = (
    valid
    .withColumn("currency_sum_currency_count_tier_value", F.sum("currency_count_tier_value").over(w2))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w3))
)

report = (
    with_windows_price_tiers
    .select(
        "currency",
        "tier_value",
        "plan_max_tier_value_times_tier_value",
        "currency_sum_currency_count_tier_value",
        "tier_value_percent_rank",
    )
    .orderBy(F.col("tier_value").desc())
)

result = (
    report
    .select(
        "currency",
        "tier_value",
        "plan_max_tier_value_times_tier_value",
        "currency_sum_currency_count_tier_value",
        "tier_value_percent_rank",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "GROUP_B").otherwise("EXTENDED").alias("currency_zone"),
    )
)

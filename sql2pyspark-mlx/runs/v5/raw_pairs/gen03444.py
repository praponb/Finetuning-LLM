from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("plan").orderBy("tier_value")
w3 = Window.partitionBy("currency")
w_ordered = Window.orderBy(F.col("days_since_effective_from_times_currency_max_tier_value").desc_nulls_last())
w5 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.avg("tier_value").over(w_hist).alias("plan_avg_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.max("tier_value").over(w3).alias("currency_max_tier_value"),
    )
)

cleaned = (
    enriched
    .filter(F.col("effective_from") >= F.lit("2024-03-19").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

base = (
    cleaned
    .filter((F.col("effective_from") >= F.lit("2024-02-08").cast("date")) & (F.col("days_since_effective_from") >= 44))
    .select(
        "plan",
        "effective_from",
        "tier_value",
        "plan_avg_tier_value",
        "tier_value_dense_rank",
        "currency_max_tier_value",
        (F.col("days_since_effective_from") * F.col("currency_max_tier_value")).alias("days_since_effective_from_times_currency_max_tier_value"),
    )
)

enriched_price_tiers = (
    base
    .filter(F.col("plan") == "basic")
    .select(
        "plan",
        "effective_from",
        "plan_avg_tier_value",
        "currency_max_tier_value",
        F.rank().over(w_ordered).alias("days_since_effective_from_times_currency_max_tier_value_rank"),
        F.avg("tier_value").over(w5).alias("plan_avg_tier_value_2"),
    )
)

result = (
    enriched_price_tiers
    .groupBy("plan")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("currency_max_tier_value", [0.25, 0.5, 0.75]).alias("currency_max_tier_value_quartiles"),
    )
    .orderBy(F.col("plan").asc_nulls_last())
)

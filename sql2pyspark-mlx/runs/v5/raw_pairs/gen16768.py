from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("tier_value")
w_hist = Window.partitionBy("currency")
w3 = Window.orderBy("tier_value_percent_rank")

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

cleaned = (
    flattened
    .filter(~F.col("currency").isin(["USD", "EUR"]))
    .select("*", F.upper("currency").alias("currency_upper"))
)

enriched = (
    cleaned
    .filter(F.col("effective_from") < "2025-10-01")
    .select(
        "*",
        F.dense_rank().over(w_running).alias("tier_value_dense_rank"),
        F.percent_rank().over(w_running).alias("tier_value_percent_rank"),
    )
)

filtered = (
    enriched
    .filter((F.col("effective_from") >= F.lit("2024-05-12").cast("date")) & F.col("tier_value_dense_rank").between(5, 6))
    .select("*", F.month("effective_from").alias("effective_from_month"))
)

valid = (
    filtered
    .filter(F.col("tier_value_dense_rank") == 4)
    .select("*", F.month("effective_from").alias("effective_from_month_2"))
)

result = (
    valid
    .select(
        "id",
        "currency_upper",
        "tier_value_dense_rank",
        "tier_value_percent_rank",
        "effective_from_month_2",
        F.avg("tier_value_percent_rank").over(w_hist).alias("currency_avg_tier_value_percent_rank"),
        F.count("tier_value_dense_rank").over(w_hist).alias("currency_count_tier_value_dense_rank"),
        F.sum("tier_value_dense_rank").over(w_hist).alias("currency_sum_tier_value_dense_rank"),
        F.max("effective_from_month_2").over(w_hist).alias("currency_max_effective_from_month_2"),
        F.dense_rank().over(w3).alias("tier_value_percent_rank_dense_rank"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan", "currency")
w2 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w_running = Window.partitionBy("plan")
w4 = Window.partitionBy("currency")

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

enriched = (
    flattened
    .select(
        "*",
        F.sum("tier_value").over(w_ordered).alias("plan_sum_tier_value"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.avg("tier_value").over(w_running).alias("plan_avg_tier_value"),
        F.avg("tier_value").over(w_running).alias("plan_avg_tier_value_2"),
    )
)

base = (
    enriched
    .filter(F.col("effective_from") >= F.lit("2024-09-17").cast("date"))
    .select(
        "plan",
        "currency",
        "tier_value",
        "plan_sum_tier_value",
        "plan_avg_tier_value",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

enriched_price_tiers = (
    base
    .withColumn("plan_max_days_since_effective_from", F.max("days_since_effective_from").over(w_running))
    .withColumn("currency_count_tier_value", F.count("tier_value").over(w4))
    .withColumn("currency_max_plan_sum_tier_value", F.max("plan_sum_tier_value").over(w4))
)

base_price_tiers = (
    enriched_price_tiers
    .filter(F.col("plan_sum_tier_value").between(186, 236))
    .select(
        "plan",
        "currency",
        "plan_sum_tier_value",
        "plan_avg_tier_value",
        "days_since_effective_from",
        "plan_max_days_since_effective_from",
        F.greatest("days_since_effective_from", "plan_max_days_since_effective_from").alias("greatest_days_since_effective_from_plan_max_days_since_effective_from"),
        (F.col("plan_avg_tier_value") * F.col("tier_value")).alias("plan_avg_tier_value_times_tier_value"),
    )
)

filtered = (
    base_price_tiers
    .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
    .select(
        "plan",
        "currency",
        "plan_sum_tier_value",
        "plan_avg_tier_value",
        "plan_max_days_since_effective_from",
        "greatest_days_since_effective_from_plan_max_days_since_effective_from",
        "plan_avg_tier_value_times_tier_value",
        F.upper("currency").alias("currency_upper"),
    )
)

filtered_price_tiers = (
    filtered
    .filter(F.col("plan") == "starter")
    .withColumn("plan_max_days_since_effective_from_times_plan_avg_tier_value", (F.col("plan_max_days_since_effective_from") * F.col("plan_avg_tier_value")))
    .withColumn("plan_avg_tier_value_times_tier_value_tier", F.when(F.col("plan_avg_tier_value_times_tier_value") >= 83, "High").when(F.col("plan_avg_tier_value_times_tier_value") >= 20, "Medium").otherwise("Low"))
)

result = (
    filtered_price_tiers
    .withColumn("plan_max_days_since_effective_from_tier", F.when(F.col("plan_max_days_since_effective_from") >= 74, "LARGE").otherwise("SMALL"))
    .cube("plan_max_days_since_effective_from_tier")
    .agg(
        F.percentile_approx("plan_sum_tier_value", [0.25, 0.5, 0.75]).alias("plan_sum_tier_value_quartiles"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.sum(F.when(F.col("currency") == "THB", F.col("plan_avg_tier_value")).otherwise(0)).alias("thb_plan_avg_tier_value"),
        F.grouping("plan_max_days_since_effective_from_tier").alias("grouping_level"),
    )
)

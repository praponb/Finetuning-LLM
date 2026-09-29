from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

filtered = (
    flattened
    .filter(F.col("plan").isin(["enterprise", "starter"]))
    .withColumn("currency_prefix", F.substring("currency", 1, 4))
    .withColumn("currency_upper", F.upper("currency"))
)

with_windows = (
    filtered
    .select(
        "plan",
        "tier_value",
        "currency_upper",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
    )
)

base = (
    with_windows
    .filter(F.col("tier_value_percent_rank") >= 0.57)
    .select(
        "plan",
        "currency_upper",
        "tier_value_percent_rank",
        "tier_value_cume_dist",
        (F.col("tier_value_cume_dist") + F.col("tier_value_percent_rank")).alias("tier_value_cume_dist_plus_tier_value_percent_rank"),
        F.round("tier_value_cume_dist", 1).alias("tier_value_cume_dist_rounded"),
    )
)

stats = (
    base
    .groupBy(
        "plan",
        F.when(F.col("tier_value_cume_dist_rounded") >= 65, "PREMIUM").otherwise("STANDARD").alias("tier_value_cume_dist_rounded_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.round(F.avg("tier_value_cume_dist_plus_tier_value_percent_rank"), 2).alias("avg_tier_value_cume_dist_plus_tier_value_percent_rank"),
        F.round(F.sum("tier_value_cume_dist_plus_tier_value_percent_rank") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("tier_value_cume_dist_plus_tier_value_percent_rank_per_enterprise"),
    )
    .filter(F.col("tier_value_cume_dist_plus_tier_value_percent_rank_per_enterprise") > 0)
)

result = (
    stats
    .select(
        "plan",
        "tier_value_cume_dist_rounded_tier",
        "pro_count",
        "avg_tier_value_cume_dist_plus_tier_value_percent_rank",
        "tier_value_cume_dist_plus_tier_value_percent_rank_per_enterprise",
    )
    .orderBy(F.col("pro_count").desc())
)

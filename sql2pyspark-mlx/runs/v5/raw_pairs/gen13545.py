from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy("plan_avg_plan_avg_tier_value")
w3 = Window.partitionBy("plan").orderBy(F.col("plan_avg_tier_value").desc_nulls_first())
w4 = Window.orderBy(F.col("plan_avg_plan_avg_tier_value_whole").asc_nulls_last())

lines = df_price_tiers.select("id", "plan", F.explode("discounts").alias("tier_value"))

with_windows = lines.select("id", "plan", F.avg("tier_value").over(w).alias("plan_avg_tier_value"))

remaining_keys = (
    with_windows
    .select("plan", "id")
    .subtract(
        df_price_tiers
        .filter(~F.col("plan").isin(["enterprise", "basic"]))
        .select("plan", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("plan_stddev_samp_plan_avg_tier_value", F.stddev_samp("plan_avg_tier_value").over(w))
    .withColumn("plan_avg_plan_avg_tier_value", F.avg("plan_avg_tier_value").over(w))
)

filtered = (
    scored
    .filter(F.col("plan_stddev_samp_plan_avg_tier_value").between(34, 64))
    .withColumn("plan_avg_plan_avg_tier_value_whole", F.floor("plan_avg_plan_avg_tier_value"))
)

matched_keys = (
    filtered
    .select("plan", "id")
    .intersect(
        df_price_tiers
        .filter(F.year("effective_from") == 2025)
        .select("plan", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .filter(F.col("plan_avg_plan_avg_tier_value") > 53)
    .select(
        "plan_avg_tier_value",
        "plan_stddev_samp_plan_avg_tier_value",
        "plan_avg_plan_avg_tier_value",
        "plan_avg_plan_avg_tier_value_whole",
        F.rank().over(w2).alias("plan_avg_plan_avg_tier_value_rank"),
        F.dense_rank().over(w3).alias("plan_avg_tier_value_dense_rank"),
        F.min("plan_avg_tier_value").over(w).alias("plan_min_plan_avg_tier_value"),
        F.cume_dist().over(w4).alias("plan_avg_plan_avg_tier_value_whole_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "plan_avg_tier_value",
        "plan_avg_plan_avg_tier_value",
        "plan_avg_plan_avg_tier_value_whole",
        "plan_avg_tier_value_dense_rank",
        "plan_min_plan_avg_tier_value",
        "plan_avg_plan_avg_tier_value_whole_cume_dist",
    )
    .orderBy(F.col("plan_avg_tier_value").asc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w_part = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

matched_keys = (
    flattened
    .select("plan", "id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan").isin(["starter", "basic", "enterprise"]))
        .select("plan", "id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("plan_sum_tier_value", F.sum("tier_value").over(w_part))
    .withColumn("plan_sum_tier_value_2", F.sum("tier_value").over(w_part))
)

cleaned = (
    enriched
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-10-18"))) & (F.col("plan_sum_tier_value") <= 335))
    .select(
        "id",
        "plan",
        "tier_value",
        "tier_value_percent_rank",
        "plan_sum_tier_value",
        "plan_sum_tier_value_2",
        F.upper("plan").alias("plan_upper"),
        (F.col("tier_value_percent_rank") / F.nullif("tier_value", F.lit(0))).alias("tier_value_percent_rank_per_tier_value"),
    )
)

result = (
    cleaned
    .select("id", "plan_sum_tier_value", "plan_upper")
    .orderBy("id", "plan_sum_tier_value")
)

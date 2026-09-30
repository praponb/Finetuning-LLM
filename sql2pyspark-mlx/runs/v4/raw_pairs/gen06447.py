from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

metrics = (
    flattened
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
)

valid = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-03-16").cast("date"))
    .withColumn("plan_group", F.when(F.col("plan").isin(["starter", "pro"]), "Core").otherwise("Other"))
)

valid_price_tiers = (
    valid
    .filter(~F.col("plan").isin(["starter", "pro", "enterprise"]))
    .select("*", F.date_add("effective_from", 45).alias("effective_from_plus_45d"))
)

metrics_price_tiers = (
    valid_price_tiers
    .select(
        "*",
        F.round((F.col("tier_value_rank") / F.sum("tier_value_rank").over(w3)), 4).alias("tier_value_rank_share"),
        F.avg("tier_value").over(w3).alias("plan_avg_tier_value"),
        F.avg("tier_value_rank").over(w3).alias("plan_avg_tier_value_rank"),
        F.count("tier_value").over(w3).alias("plan_count_tier_value"),
    )
)

base = (
    metrics_price_tiers
    .filter(F.col("plan_avg_tier_value") != 62)
    .withColumn("plan_avg_tier_value_filled", F.coalesce("plan_avg_tier_value", F.lit(0)))
)

remaining_keys = (
    base
    .select("plan", "id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan") != "starter")
        .select("plan", "id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "tier_value",
        "tier_value_rank",
        "tier_value_cume_dist",
        "plan_group",
        "effective_from_plus_45d",
        "tier_value_rank_share",
        "plan_avg_tier_value_rank",
    )
    .orderBy("tier_value")
)

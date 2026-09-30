from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan").orderBy(F.col("is_enterprise").asc_nulls_last())
w_grp = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

filtered = (
    flattened
    .filter(F.col("effective_from") > "2024-11-01")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        F.when(F.col("currency").isin(["USD", "THB"]), "Group A").otherwise("Other").alias("currency_group"),
    )
)

filtered_price_tiers = (
    filtered
    .filter((F.col("effective_from") >= F.lit("2024-06-06").cast("date")) & F.month("effective_from").isin([2, 6, 9]))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        "currency_group",
        F.round("tier_value", 2).alias("tier_value_rounded"),
        F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"),
    )
)

filtered_price_tiers_2 = (
    filtered_price_tiers
    .filter(F.col("tier_value") >= 49)
    .select(
        "id",
        "plan",
        "effective_from",
        "currency_group",
        "tier_value_rounded",
        "tier_value_filled",
        F.when(F.col("plan") == "enterprise", 1).otherwise(0).alias("is_enterprise"),
    )
)

metrics = (
    filtered_price_tiers_2
    .withColumn("is_enterprise_rank", F.rank().over(w_running))
    .withColumn("plan_count_is_enterprise", F.count("is_enterprise").over(w_grp))
    .withColumn("plan_max_tier_value_filled", F.max("tier_value_filled").over(w_grp))
    .withColumn("plan_min_tier_value_filled", F.min("tier_value_filled").over(w_grp))
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "effective_from",
        "currency_group",
        "tier_value_filled",
        "is_enterprise_rank",
        "plan_count_is_enterprise",
        "plan_max_tier_value_filled",
        F.when(F.col("plan").rlike("^(basic|pro)$"), "CORE").when(F.col("plan").rlike("^(starter|enterprise)$"), "EXTENDED").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy("id", F.col("plan_max_tier_value_filled").desc())
)

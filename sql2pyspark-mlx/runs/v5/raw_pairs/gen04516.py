from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w_ordered = Window.partitionBy("plan").orderBy(F.col("tier_value_rounded").desc_nulls_first())
w3 = Window.partitionBy("plan")

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

filtered = (
    exploded
    .filter((F.col("effective_from") >= F.lit("2024-04-29").cast("date")) & F.col("plan").isin(["starter", "enterprise", "basic"]))
    .select(
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        F.substring("currency", 1, 3).alias("currency_prefix"),
    )
)

cleaned = (
    filtered
    .filter(F.col("plan").isin(["enterprise", "pro", "starter"]))
    .withColumn("effective_from_label", F.date_format("effective_from", "yyyy-MM"))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("effective_from").between("2024-10-01", "2025-01-01"))
    .select(
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        "currency_prefix",
        "effective_from_label",
        F.initcap(F.trim("currency")).alias("currency_title"),
        F.round("tier_value", 2).alias("tier_value_rounded"),
    )
)

enriched = (
    cleaned_price_tiers
    .select(
        "*",
        F.min("tier_value_rounded").over(w).alias("currency_min_tier_value_rounded"),
        F.rank().over(w_ordered).alias("tier_value_rounded_rank"),
        F.max("tier_value_rounded").over(w3).alias("plan_max_tier_value_rounded"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("currency_min_tier_value_rounded") >= 67, "LARGE").otherwise("SMALL").alias("currency_min_tier_value_rounded_tier"),
        F.when(F.col("tier_value_rounded_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("tier_value_rounded_rank_tier"),
    )
    .agg(
        F.percentile_approx("plan_max_tier_value_rounded", 0.95).alias("plan_max_tier_value_rounded_p95"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("currency_min_tier_value_rounded"))).alias("enterprise_total_currency_min_tier_value_rounded"),
    )
    .filter(F.col("enterprise_total_currency_min_tier_value_rounded") >= 0)
)

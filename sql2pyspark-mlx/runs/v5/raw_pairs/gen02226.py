from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

scored = flattened.withColumn("currency_sum_tier_value", F.sum("tier_value").over(w_hist))

base = (
    scored
    .filter(F.col("currency_sum_tier_value") != 237)
    .select(
        "plan",
        "currency",
        "tier_value",
        "currency_sum_tier_value",
        F.col("currency_sum_tier_value").cast("string").alias("currency_sum_tier_value_str"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("plan") == "pro")
    .select(
        "plan",
        "currency",
        "tier_value",
        "currency_sum_tier_value",
        "currency_sum_tier_value_str",
        F.least("tier_value", "currency_sum_tier_value").alias("least_tier_value_currency_sum_tier_value"),
        (F.col("tier_value") + F.col("currency_sum_tier_value")).alias("tier_value_plus_currency_sum_tier_value"),
    )
)

result = (
    base_price_tiers
    .groupBy(
        F.when(F.col("least_tier_value_currency_sum_tier_value") >= 75, "PREMIUM").otherwise("STANDARD").alias("least_tier_value_currency_sum_tier_value_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("tier_value_per_enterprise"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("tier_value"))).alias("eur_avg_tier_value"),
    )
    .orderBy(F.col("tier_value_per_enterprise").desc())
)

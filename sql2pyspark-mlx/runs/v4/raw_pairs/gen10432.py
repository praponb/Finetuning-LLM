from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w_ordered = Window.orderBy(F.col("tier_value_rounded").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("plan", "currency")
w6 = Window.partitionBy("currency", "plan")
w7 = Window.partitionBy("currency").orderBy("tier_value_rounded")
w8 = Window.partitionBy("currency", "plan").orderBy(F.col("tier_value_rounded_rank").asc_nulls_last())
w9 = Window.orderBy(F.col("tier_value_rounded_cume_dist").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("tier_value"))
)

cleaned = (
    lines
    .filter(F.col("plan") == "pro")
    .withColumn("tier_value_rounded", F.round("tier_value", 2))
    .withColumn("clean_plan", F.trim("plan"))
)

enriched = (
    cleaned
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("tier_value_rounded_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w3))
)

matched_keys = (
    enriched
    .select("plan", "currency")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
        .select("plan", "currency")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "tier_value_rounded",
        "tier_value_percent_rank",
        "tier_value_rounded_percent_rank",
        "tier_value_dense_rank",
        F.when(F.col("tier_value") >= 67, "High").when(F.col("tier_value") >= 14, "Medium").otherwise("Low").alias("tier_value_tier"),
    )
)

cleaned_price_tiers = (
    filtered
    .filter(F.col("plan") == "pro")
    .select(
        "*",
        F.when(F.col("tier_value_rounded") >= 90, "High").when(F.col("tier_value_rounded") >= 38, "Medium").otherwise("Low").alias("tier_value_rounded_level"),
    )
)

scored = (
    cleaned_price_tiers
    .withColumn("tier_value_rounded_rank", F.rank().over(w_ordered))
    .withColumn("plan_stddev_samp_tier_value_rounded", F.stddev_samp("tier_value_rounded").over(w4))
    .withColumn("plan_max_tier_value_rounded_percent_rank", F.max("tier_value_rounded_percent_rank").over(w5))
    .withColumn("currency_max_tier_value_rounded_percent_rank", F.max("tier_value_rounded_percent_rank").over(w6))
    .withColumn("tier_value_rounded_cume_dist", F.cume_dist().over(w7))
)

result = (
    scored
    .withColumn("tier_value_rounded_rank_cume_dist", F.cume_dist().over(w8))
    .withColumn("tier_value_rounded_cume_dist_percent_rank", F.percent_rank().over(w9))
    .withColumn("currency_count_tier_value", F.count("tier_value").over(w6))
)

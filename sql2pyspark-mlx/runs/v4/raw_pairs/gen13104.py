from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy("tier_value")
w4 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w5 = Window.partitionBy("week_start").orderBy(F.col("avg_tier_value").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

scored = (
    flattened
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("tier_value_rank", F.rank().over(w2))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w3))
    .withColumn("tier_value_percent_rank_2", F.percent_rank().over(w4))
)

kept_keys = (
    scored
    .select("currency", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("currency", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("tier_value_percent_rank_2").between(0.53, 0.55))
    .select(
        "*",
        F.when(F.col("tier_value_percent_rank") >= 0.86, "High").when(F.col("tier_value_percent_rank") >= 0.13, "Medium").otherwise("Low").alias("tier_value_percent_rank_level"),
    )
)

base = (
    cleaned
    .filter(F.col("effective_from") > F.lit("2024-09-04").cast("date"))
    .select(
        "effective_from",
        "currency",
        "tier_value",
        "tier_value_cume_dist",
        "tier_value_rank",
        "tier_value_percent_rank",
        "tier_value_percent_rank_level",
        (F.col("tier_value_percent_rank_2") * F.col("tier_value")).alias("tier_value_percent_rank_2_times_tier_value"),
        F.lower("currency").alias("currency_lower"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"), "currency")
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.percentile_approx("tier_value_cume_dist", [0.25, 0.5, 0.75]).alias("tier_value_cume_dist_quartiles"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .withColumn("avg_tier_value_percent_rank", F.percent_rank().over(w5))
)

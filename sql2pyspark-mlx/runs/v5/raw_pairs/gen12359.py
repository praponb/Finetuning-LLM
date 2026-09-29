from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tier_value_filled")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy("tier_value_plus_tier_value_filled")

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_prices").alias("tier_value"))
)

filtered = (
    flattened
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-06-09"))) & F.month("effective_from").isin([1, 3, 5]))
    .withColumn("tier_value_filled", F.coalesce("tier_value", F.lit(0)))
)

filtered_price_tiers = (
    filtered
    .filter((F.col("effective_from") > F.lit("2024-02-07").cast("date")) & (F.col("tier_value") >= 65))
    .select(
        "*",
        (F.col("tier_value") + F.col("tier_value_filled")).alias("tier_value_plus_tier_value_filled"),
    )
)

enriched = (
    filtered_price_tiers
    .filter(F.col("tier_value_plus_tier_value_filled") > 40)
    .select(
        "*",
        F.cume_dist().over(w).alias("tier_value_filled_cume_dist"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") != "pro")
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_price_tiers_2 = (
    verified
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-06-09")))
    .withColumn("effective_from_plus_59d", F.date_add("effective_from", 59))
)

scored = (
    filtered_price_tiers_2
    .select(
        "*",
        F.dense_rank().over(w3).alias("tier_value_plus_tier_value_filled_dense_rank"),
    )
)

result = (
    scored
    .rollup(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        F.when(F.col("tier_value_cume_dist") >= 0.74, "HIGH").otherwise("LOW").alias("tier_value_cume_dist_tier"),
        F.when(F.col("tier_value_plus_tier_value_filled_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("tier_value_plus_tier_value_filled_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("tier_value_filled_cume_dist", 0.95).alias("tier_value_filled_cume_dist_p95"),
        F.percentile_approx("tier_value_filled_cume_dist", [0.25, 0.5, 0.75]).alias("tier_value_filled_cume_dist_quartiles"),
        F.min("tier_value_plus_tier_value_filled_dense_rank").alias("min_tier_value_plus_tier_value_filled_dense_rank"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("tier_value_filled_cume_dist_p95") < 1000000)
    .orderBy(F.col("activity_month").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_prices").alias("tier_value"))
)

metrics = (
    flattened
    .filter(F.col("tier_value") >= 44)
    .select(
        "id",
        "tier_value",
        F.max("tier_value").over(w).alias("currency_max_tier_value"),
        F.max("tier_value").over(w).alias("currency_max_tier_value_2"),
        F.stddev_samp("tier_value").over(w).alias("currency_stddev_samp_tier_value"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "enterprise")
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("currency_max_tier_value_2") == 29)
    .select(
        "id",
        "tier_value",
        "currency_max_tier_value",
        "currency_max_tier_value_2",
        "currency_stddev_samp_tier_value",
        (F.col("currency_max_tier_value_2") + F.col("currency_max_tier_value")).alias("currency_max_tier_value_2_plus_currency_max_tier_value"),
        F.col("currency_max_tier_value").cast("string").alias("currency_max_tier_value_num"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("currency_max_tier_value_2_plus_currency_max_tier_value") >= 80, "LARGE").otherwise("SMALL").alias("currency_max_tier_value_2_plus_currency_max_tier_value_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("currency_stddev_samp_tier_value"), 2).alias("avg_currency_stddev_samp_tier_value"),
        F.percentile_approx("currency_max_tier_value", [0.25, 0.5, 0.75]).alias("currency_max_tier_value_quartiles"),
        F.percentile_approx("currency_max_tier_value_2_plus_currency_max_tier_value", [0.25, 0.5, 0.75]).alias("currency_max_tier_value_2_plus_currency_max_tier_value_quartiles"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("currency_stddev_samp_tier_value", [0.25, 0.5, 0.75]).alias("currency_stddev_samp_tier_value_quartiles"),
    )
    .filter(F.col("num_rows") > 1)
)

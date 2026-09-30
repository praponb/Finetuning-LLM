from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

valid = (
    flattened
    .filter(F.col("effective_from") > F.lit("2024-06-04").cast("date"))
    .withColumn("plan_group", F.when(F.col("plan").isin(["basic", "starter"]), "Tier 1").otherwise("Other"))
)

enriched = (
    valid
    .filter(F.col("effective_from") >= "2025-01-01")
    .select("*", F.min("val").over(w).alias("currency_min_val"))
)

filtered = (
    enriched
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-03-19"))) & (F.col("val") <= 23))
    .withColumn("val_minus_currency_min_val", (F.col("val") - F.col("currency_min_val")))
    .withColumn("greatest_val_currency_min_val", F.greatest("val", "currency_min_val"))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("val_minus_currency_min_val").between(28, 33))
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        "currency_min_val",
        "greatest_val_currency_min_val",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from_2"),
    )
)

valid_price_tiers = (
    filtered_price_tiers
    .filter(F.col("currency_min_val") <= 61)
    .select(
        "*",
        (F.col("greatest_val_currency_min_val") * F.col("days_since_effective_from_2")).alias("greatest_val_currency_min_val_times_days_since_effective_from_2"),
    )
)

kept_keys = (
    valid_price_tiers
    .select("currency", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "starter")
        .select("currency", "id")
    )
)

kept_rows = (
    valid_price_tiers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.round(F.sum("days_since_effective_from") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("days_since_effective_from_per_thb"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.round(F.avg("greatest_val_currency_min_val_times_days_since_effective_from_2"), 2).alias("avg_greatest_val_currency_min_val_times_days_since_effective_from_2"),
    )
    .orderBy(
        F.col("avg_greatest_val_currency_min_val_times_days_since_effective_from_2").desc_nulls_last(),
    )
)

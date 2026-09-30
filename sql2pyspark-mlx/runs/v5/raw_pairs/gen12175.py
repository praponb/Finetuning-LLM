from pyspark.sql import functions as F

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

base = (
    flattened
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-03-23")))
    .select(
        "plan",
        "currency",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
        F.coalesce("val", F.lit(0)).alias("val_filled"),
    )
)

kept_keys = (
    base
    .select("plan", "currency")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from") >= "2024-08-01")
        .select("plan", "currency")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("val_filled") > 29)
    .withColumn("days_since_effective_from_adjusted", F.round((F.col("days_since_effective_from") * 1.07), 2))
)

result = (
    filtered
    .groupBy(
        "currency",
        F.when(F.col("days_since_effective_from") >= 85, "HIGH").otherwise("LOW").alias("days_since_effective_from_tier"),
        F.when(F.col("val_filled") >= 66, "HIGH").otherwise("LOW").alias("val_filled_tier"),
    )
    .agg(
        F.percentile_approx("days_since_effective_from", 0.5).alias("days_since_effective_from_p50"),
        F.percentile_approx("days_since_effective_from", [0.25, 0.5, 0.75]).alias("days_since_effective_from_quartiles"),
        F.percentile_approx("val_filled", 0.95).alias("val_filled_p95"),
    )
    .orderBy(F.col("val_filled_p95").asc_nulls_last())
)

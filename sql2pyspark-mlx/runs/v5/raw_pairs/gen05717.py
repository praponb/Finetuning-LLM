from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

cleaned = (
    flattened
    .filter(F.col("effective_from") >= F.lit("2024-02-22").cast("date"))
    .select(
        "*",
        F.when(F.col("val") >= 90, "High").when(F.col("val") >= 25, "Medium").otherwise("Low").alias("val_tier"),
    )
)

filtered = (
    cleaned
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-06-15")))
    .withColumn("effective_from_plus_months", F.add_months("effective_from", 6))
)

kept_keys = (
    filtered
    .select("currency", "id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from") > "2024-08-07")
        .select("currency", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("plan")
    .pivot("currency", ["EUR", "USD", "THB"])
    .agg(F.sum("val").alias("total"))
)

from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("element"))
)

kept_keys = (
    exploded
    .select("currency", "id")
    .intersectAll(
        df_price_tiers
        .filter(~F.col("currency").isin(["USD", "EUR"]))
        .select("currency", "id")
    )
)

verified = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("currency") == "EUR")
    .select(
        "*",
        F.when(F.col("element") >= 64, "High").when(F.col("element") >= 20, "Medium").otherwise("Low").alias("element_level"),
        F.length("currency").alias("currency_length"),
    )
)

base = (
    cleaned
    .filter(F.col("currency_length") != 58)
    .select(
        "id",
        "currency",
        "element",
        "currency_length",
        F.col("currency_length").cast("string").alias("currency_length_str"),
        F.when(F.col("currency").isin(["THB", "EUR"]), "Priority").otherwise("Other").alias("currency_group"),
    )
)

cleaned_price_tiers = (
    base
    .filter(F.col("currency_length") < 72)
    .select(
        "*",
        F.round("currency_length", 0).alias("currency_length_rounded"),
        F.upper("currency").alias("currency_upper"),
    )
)

result = (
    cleaned_price_tiers
    .withColumn("currency_length_tier", F.when(F.col("currency_length") >= 74, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["currency", "currency_length_tier"], ["currency"]],
        "currency",
        "currency_length_tier",
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("currency_length_rounded"))).alias("eur_total_currency_length_rounded"),
        F.round(F.avg("currency_length"), 2).alias("avg_currency_length"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
    )
)

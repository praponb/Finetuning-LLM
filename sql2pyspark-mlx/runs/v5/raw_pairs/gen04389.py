from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

valid = (
    exploded
    .filter((F.col("effective_from") > F.lit("2024-04-25").cast("date")) & (F.col("currency") != "THB"))
    .withColumn("currency_upper", F.upper("currency"))
)

cleaned = (
    valid
    .filter(F.col("val").between(36, 65))
    .select(
        "effective_from",
        "currency",
        "val",
        "currency_upper",
        F.substring("currency", 1, 2).alias("currency_prefix"),
        F.trim("currency").alias("clean_currency"),
    )
)

matched_keys = (
    cleaned
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(~F.col("plan").isin(["basic", "pro", "enterprise"]))
        .select("currency")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "currency",
        "val",
        "currency_upper",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "PRIORITY").when(F.col("currency").rlike("^(USD)$"), "EXTENDED").otherwise("GROUP_A").alias("currency_zone"),
    )
    .orderBy(F.col("currency").desc())
)

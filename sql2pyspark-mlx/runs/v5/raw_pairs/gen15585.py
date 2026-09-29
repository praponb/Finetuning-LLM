from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

filtered = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-03-17").cast("date"))
    .select(
        "plan",
        "effective_from",
        "currency",
        "element",
        F.floor("element").alias("element_whole"),
    )
)

remaining_keys = (
    filtered
    .select("currency", "plan")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency") != "USD")
        .select("currency", "plan")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("currency", "plan")
    .intersect(
        df_price_tiers
        .filter(F.col("currency") == "EUR")
        .select("currency", "plan")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("effective_from") >= F.lit("2024-02-26").cast("date")) & (F.col("element_whole") >= 51))
    .select("*", F.lower("currency").alias("currency_lower"))
)

result = (
    cleaned
    .select(
        "plan",
        "effective_from",
        "element",
        "element_whole",
        "currency_lower",
        F.when(F.col("plan").rlike("^(pro|starter)$"), "GROUP_A").otherwise("CORE").alias("plan_zone"),
    )
    .orderBy(F.col("effective_from").asc_nulls_last())
)

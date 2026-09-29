from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("val"))
)

filtered = (
    lines
    .filter(F.col("plan") != "enterprise")
    .withColumn("val_whole", F.ceil("val"))
)

cleaned = (
    filtered
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-06-10"))) & F.col("plan").isin(["enterprise", "basic"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        "val_whole",
        (F.col("val_whole") - F.col("val")).alias("val_whole_minus_val"),
    )
)

kept_keys = (
    cleaned
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.month("effective_from").isin([1, 10, 12]))
        .select("id", "plan")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("effective_from").between("2023-07-11", "2025-07-01"))
    .select(
        "plan",
        F.date_trunc("YEAR", "effective_from").alias("effective_from_period"),
    )
)

totals = (
    valid
    .groupBy(
        "plan",
        F.date_trunc("QUARTER", "effective_from_period").alias("quarter_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("effective_from_period")) - F.unix_timestamp(F.min("effective_from_period"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
    )
)

result = totals.select("plan", "quarter_start", "span_minutes", "cnt")

from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("tier_value").between(71, 75))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        F.date_format("effective_from", "MMM yyyy").alias("effective_from_label"),
    )
)

filtered_price_tiers = (
    filtered
    .filter(F.col("plan") == "basic")
    .select("*", F.initcap(F.trim("plan")).alias("plan_title"))
)

filtered_price_tiers_2 = (
    filtered_price_tiers
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-02-28"))) & F.col("currency").isin(["EUR", "USD"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "effective_from_label",
        "plan_title",
        F.when(F.col("plan") == "enterprise", 1).otherwise(0).alias("is_enterprise"),
        F.lower("currency").alias("currency_lower"),
    )
)

remaining_keys = (
    filtered_price_tiers_2
    .select("plan", "id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("plan", "id")
    )
)

kept_rows = (
    filtered_price_tiers_2
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .rollup("plan", "activity_month")
    .agg(
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.sum(F.when(F.col("currency") == "THB", F.col("is_enterprise")).otherwise(0)).alias("thb_is_enterprise"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.grouping("plan").alias("gid"),
    )
    .orderBy(F.col("thb_count").desc())
)

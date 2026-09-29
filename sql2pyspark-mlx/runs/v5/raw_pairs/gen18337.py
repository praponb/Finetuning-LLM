from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("element"))
)

cleaned = (
    flattened
    .filter((F.col("effective_from") > F.lit("2024-05-26").cast("date")) & ~F.col("plan").isin(["pro", "enterprise"]))
    .select("*", F.length("plan").alias("plan_length"))
)

base = (
    cleaned
    .filter(F.col("element") > 46)
    .select(
        "plan",
        "effective_from",
        F.round("element", 1).alias("element_rounded"),
        F.round((F.col("element") * 0.9), 2).alias("element_scaled"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("element_rounded") > 61)
    .select(
        "*",
        F.when(F.col("element_rounded") >= 61, "High").when(F.col("element_rounded") >= 18, "Medium").otherwise("Low").alias("element_rounded_band"),
        F.year("effective_from").alias("effective_from_year"),
    )
)

cleaned_price_tiers = (
    base_price_tiers
    .filter(F.col("effective_from_year") == 26)
    .select(
        "effective_from",
        "element_scaled",
        "element_rounded_band",
        "effective_from_year",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
        (F.col("element_rounded") - F.col("effective_from_year")).alias("element_rounded_minus_effective_from_year"),
    )
)

result = (
    cleaned_price_tiers
    .groupBy(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
    )
    .agg(
        F.min("element_scaled").alias("min_element_scaled"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("min_element_scaled").asc_nulls_last())
)

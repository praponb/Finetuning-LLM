from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("element"),
    )
)

valid = (
    lines
    .filter((F.col("effective_from") >= F.lit("2024-06-04").cast("date")) & (F.col("element") > 69))
    .select("*", F.lower("plan").alias("plan_lower"))
)

cleaned = (
    valid
    .filter(F.month("effective_from").isin([3, 4, 12]))
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        "plan_lower",
        F.round((F.col("element") * 1.15), 2).alias("element_scaled"),
    )
)

base = (
    cleaned
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-06-18")))
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        "plan_lower",
        F.date_format("effective_from", "MMM yyyy").alias("effective_from_label"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("effective_from") >= F.lit("2024-03-14").cast("date"))
    .select(
        "*",
        F.when(F.col("element") >= 86, "High").when(F.col("element") >= 26, "Medium").otherwise("Low").alias("element_band"),
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

result = (
    base_price_tiers
    .select("effective_from", "element", "plan_lower", "element_band")
    .orderBy(F.col("effective_from").asc_nulls_first())
)

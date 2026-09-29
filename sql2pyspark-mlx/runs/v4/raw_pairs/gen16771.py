from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("element"))
)

filtered = (
    flattened
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-07-17")))
    .select(
        "*",
        F.date_format("effective_from", "yyyy-MM").alias("effective_from_label"),
    )
)

cleaned = (
    filtered
    .filter(F.col("effective_from") >= F.lit("2024-08-03").cast("date"))
    .select("*", F.date_trunc("YEAR", "effective_from").alias("effective_from_period"))
)

result = (
    cleaned
    .select("id", "element", "effective_from_label", "effective_from_period")
    .orderBy(F.col("effective_from_period").desc())
)

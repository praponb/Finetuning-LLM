from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-19").cast("date"))
    .select(
        "*",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)

filtered = (
    cleaned
    .filter(F.col("revenue") < 3500)
    .select(
        "*",
        (F.col("greatest_revenue_units") - F.col("least_units_revenue")).alias("greatest_revenue_units_minus_least_units_revenue"),
        F.greatest("greatest_revenue_units", "least_units_revenue").alias("greatest_greatest_revenue_units_least_units_revenue"),
    )
)

cleaned_store_sales = (
    filtered
    .filter(F.col("greatest_greatest_revenue_units_least_units_revenue") < 71)
    .select(
        "*",
        F.round("greatest_revenue_units_minus_least_units_revenue", 1).alias("greatest_revenue_units_minus_least_units_revenue_rounded"),
    )
)

remaining_keys = (
    cleaned_store_sales
    .select("product_id", "store_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("product_id").isin([2, 3, 7]))
        .select("product_id", "store_id")
    )
)

verified = (
    cleaned_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "least_units_revenue",
        "greatest_greatest_revenue_units_least_units_revenue",
    )
    .orderBy(F.col("least_units_revenue").desc())
)

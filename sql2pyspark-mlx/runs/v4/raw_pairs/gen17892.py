from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-03").cast("date"))
    .select("*", F.greatest("units", "revenue").alias("greatest_units_revenue"))
)

filtered = (
    cleaned
    .filter(F.col("sale_date") > F.lit("2024-01-06").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

kept_keys = (
    filtered
    .select("id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("units") <= 11)
        .select("id", "product_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

cleaned_store_sales = (
    reconciled
    .filter(F.col("units") == 29)
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "greatest_revenue_units",
        "sale_date_month_end",
        F.last_day("sale_date_month_end").alias("sale_date_month_end_month_end"),
    )
)

result = (
    cleaned_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3500, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
    )
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)

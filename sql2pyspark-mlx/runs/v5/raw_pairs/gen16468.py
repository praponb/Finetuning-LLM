from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-04-01", "2025-06-01"))
    .select(
        "*",
        F.date_add("sale_date", 20).alias("sale_date_plus_20d"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

result = (
    cleaned
    .select("store_id", "product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("units").isNotNull())
        .select("store_id", "product_id")
    )
)

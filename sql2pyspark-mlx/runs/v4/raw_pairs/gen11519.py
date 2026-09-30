from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("units") > 14)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    cleaned
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") >= "2024-08-01")
        .select("product_id", "id")
    )
)

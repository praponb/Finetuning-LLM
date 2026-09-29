from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-01-19", "2025-09-01") & (F.col("sale_date").between("2024-10-01", "2025-01-26") | F.col("product_id").isin([9, 10])))
    .select(
        "id",
        "product_id",
        "units",
        F.round((F.col("revenue") * 0.9), 2).alias("revenue_adjusted"),
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

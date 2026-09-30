from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~((F.col("revenue") <= 1600) & (F.col("sale_date") >= "2025-05-01") & (F.col("sale_date") > "2023-12-01")))
    .select(
        "id",
        "store_id",
        "revenue",
        "sale_date",
        F.round((F.col("revenue") * 0.9), 2).alias("revenue_adjusted"),
    )
)

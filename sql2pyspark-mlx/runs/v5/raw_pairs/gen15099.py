from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2023-11-01")
    .select(
        "id",
        "product_id",
        "store_id",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)

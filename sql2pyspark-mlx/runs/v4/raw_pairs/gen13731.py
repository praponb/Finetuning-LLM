from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("store_id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date").between("2023-06-21", "2024-06-01"))
        .select("store_id", "product_id")
    )
)

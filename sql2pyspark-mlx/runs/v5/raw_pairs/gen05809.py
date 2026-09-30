from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue") <= 3300)
        .select("product_id", "id")
    )
)

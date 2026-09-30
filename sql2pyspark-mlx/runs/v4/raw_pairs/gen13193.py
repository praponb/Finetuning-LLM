from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id") == 1)
        .select("id")
    )
)

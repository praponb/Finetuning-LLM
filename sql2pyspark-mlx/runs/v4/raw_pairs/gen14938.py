from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([2, 3, 5, 10]))
        .select("product_id")
    )
)

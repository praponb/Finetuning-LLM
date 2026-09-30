from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([4, 9]))
        .select("product_id", "store_id")
    )
)

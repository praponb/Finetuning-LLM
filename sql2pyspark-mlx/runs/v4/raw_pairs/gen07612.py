from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id", "store_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([1, 3, 10]))
        .select("product_id", "store_id")
    )
)

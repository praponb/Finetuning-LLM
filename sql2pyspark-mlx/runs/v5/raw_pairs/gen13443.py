from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("store_id").isin([1, 3, 5]))
        .select("product_id")
    )
)

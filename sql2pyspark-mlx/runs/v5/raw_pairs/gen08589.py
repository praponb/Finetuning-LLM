from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .select("product_id", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("store_id").isin([3, 5, 6]))
        .select("product_id", "sale_date")
    )
    .orderBy(F.col("product_id").desc())
)

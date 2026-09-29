from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(~(F.col("ss.product_id").isin([2, 6, 7, 10]) & F.col("ss.store_id").isin([1, 2, 3, 6])))
    .select(
        F.col("p.product_name"),
        F.col("ss.product_id"),
        F.size(F.col("p.tags")).alias("tags_count"),
    )
    .orderBy(F.col("tags_count").desc())
)

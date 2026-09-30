from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.product_id").isin([1, 2]))
    .select(F.col("ss.store_id"), F.col("ss.revenue"), F.col("p.cost"))
    .orderBy(F.col("cost").desc(), F.col("store_id").desc())
)

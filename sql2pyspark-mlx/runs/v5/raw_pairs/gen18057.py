from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("price").between(660, 1080)).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("ss.units").between(17, 23))
    .select(F.col("ss.id"), F.col("ss.store_id"), F.col("ss.units"))
)

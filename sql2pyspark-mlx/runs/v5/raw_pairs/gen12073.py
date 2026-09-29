from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("stock_qty") < 381).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("ss.units").between(20, 32))
    .select(F.col("ss.id"), F.col("ss.store_id"))
    .orderBy(F.col("id").desc())
)

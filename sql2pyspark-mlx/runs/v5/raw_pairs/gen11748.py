from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id").isin([2, 6, 7]) & F.col("i.quantity").between(151, 200))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("ss.store_id"),
        F.col("ss.product_id").alias("store_sale_product_id"),
    )
    .orderBy(F.col("product_id").desc())
)

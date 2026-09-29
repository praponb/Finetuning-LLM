from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("ss.product_id").alias("store_sale_product_id"),
        F.col("ss.revenue"),
        F.floor(F.col("i.quantity")).alias("quantity_whole"),
    )
    .orderBy(F.col("revenue").desc(), F.col("id").desc())
)

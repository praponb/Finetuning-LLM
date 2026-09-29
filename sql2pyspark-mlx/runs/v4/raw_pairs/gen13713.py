from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("ss.product_id").alias("store_sale_product_id"),
    )
    .orderBy("revenue", F.col("store_sale_product_id").desc())
)

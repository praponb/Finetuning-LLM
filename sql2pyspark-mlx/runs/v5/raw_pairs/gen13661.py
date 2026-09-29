from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("i.quantity").cast("double").alias("quantity_num"),
    )
)

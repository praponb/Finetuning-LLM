from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.brand"),
        F.col("s.region"),
        F.when(F.col("i.quantity") >= 264, "High").when(F.col("i.quantity") >= 41, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

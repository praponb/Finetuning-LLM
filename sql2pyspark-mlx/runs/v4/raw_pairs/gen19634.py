from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.product_id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.when(F.col("i.quantity") >= 259, "High").when(F.col("i.quantity") >= 43, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

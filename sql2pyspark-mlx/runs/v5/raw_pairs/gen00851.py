from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.region") == "West") & F.col("s.store_name").like("%Sia%"))
    .select(
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("s.city"),
        F.col("s.region"),
    )
)

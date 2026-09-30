from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter((F.col("i.last_restock") >= "2023-04-22") & F.col("i.store_id").isin([3, 4, 5]))
    .select(
        F.col("i.quantity"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("city").desc())
)

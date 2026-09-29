from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(~F.col("p.category").isin(["Home", "Sports", "Books"]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
)

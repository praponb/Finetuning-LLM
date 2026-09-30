from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter((F.col("s.sqft") < 7801) | (F.col("i.quantity") >= 223))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("s.region"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.ceil(F.col("i.quantity")).alias("quantity_whole"),
    )
)

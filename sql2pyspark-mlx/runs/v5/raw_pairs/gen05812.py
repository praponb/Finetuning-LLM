from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("s.city").isin(["London", "Tokyo", "Berlin"]) & (F.col("s.city") == "London"))
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.opened_date"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
    )
)

from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id").isin([4, 7, 8]) | (F.col("s.opened_date") >= "2024-02-13"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("p.product_name"),
    )
)

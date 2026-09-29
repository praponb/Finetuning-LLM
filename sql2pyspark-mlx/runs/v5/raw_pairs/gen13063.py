from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.brand"),
        F.col("s.city"),
        F.date_trunc("month", F.col("p.launch_date")).alias("launch_date_period"),
    )
)

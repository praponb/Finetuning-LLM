from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("p.launch_date") > "2023-12-01")
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("p.price"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.floor(F.col("s.sqft")).alias("sqft_whole"),
    )
    .orderBy("city")
)

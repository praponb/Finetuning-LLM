from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("s.sqft").between(4505, 13332))
    .select(
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.concat_ws(",", F.col("s.store_name"), F.col("s.city")).alias("store_name_city_key"),
    )
)

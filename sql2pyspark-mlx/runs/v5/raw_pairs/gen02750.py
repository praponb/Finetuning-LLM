from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(~F.col("s.region").isin(["South", "West", "East"]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.length(F.col("s.store_name")).alias("store_name_length"),
    )
    .orderBy("product_name", "store_name_length", "id")
    .limit(5)
)

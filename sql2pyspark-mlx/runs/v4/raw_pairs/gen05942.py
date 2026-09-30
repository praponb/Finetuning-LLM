from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft") < 14574)
    .groupBy(F.col("s.city"))
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .orderBy("city")
)

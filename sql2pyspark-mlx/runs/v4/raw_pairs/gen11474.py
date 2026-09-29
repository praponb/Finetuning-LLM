from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.sqft") >= 13347, "high").when(F.col("s.sqft") >= 7598, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
    )
)

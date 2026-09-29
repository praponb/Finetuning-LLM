from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.quantity") >= 238)
    .groupBy(F.col("s.region"))
    .agg(
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("first_last_restock").desc(), F.col("avg_quantity").desc())
)

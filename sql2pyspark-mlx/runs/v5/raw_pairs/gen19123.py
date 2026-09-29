from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter(F.upper(F.trim(F.col("p.product_name"))) == "NOISE-CANCEL HEADPHONES")
    .groupBy(F.col("p.category"))
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
    .orderBy(F.col("category").desc())
)

from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.product_id").isin([1, 2, 6]) & (F.col("i.product_id") == 3))
    .select(F.col("i.id"), F.col("i.product_id"), F.col("i.store_id"), F.col("p.price"))
    .orderBy(F.col("product_id").desc(), F.col("price").desc(), "id")
    .limit(3)
)

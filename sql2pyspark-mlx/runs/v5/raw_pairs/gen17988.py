from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 4.1, "high").when(F.col("s.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.city"),
    )
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
    .orderBy("rating_bucket")
)

from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
)

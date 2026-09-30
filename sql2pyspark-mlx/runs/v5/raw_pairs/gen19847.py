from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 3.5, "high").when(F.col("s.rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("*").alias("inventory_count"))
    .filter(F.col("inventory_count") >= 5)
    .orderBy("city")
)
